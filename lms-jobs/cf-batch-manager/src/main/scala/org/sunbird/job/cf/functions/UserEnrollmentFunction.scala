package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.cf.util.{CFCacheUtil, EnrollmentApiUtil, HierarchyHelper, BatchMappingUtil}
import org.sunbird.job.cf.util.CFCacheUtil.{CLNode, CLStructure}
import org.sunbird.dp.core.util.{CassandraUtil, HttpUtil}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import com.datastax.driver.core.PreparedStatement

import scala.collection.JavaConverters._

class UserEnrollmentFunction(config: CfBatchManagerConfig)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserEnrollmentFunction])

  @transient private var cassandraUtil: CassandraUtil = _
  @transient private var hierarchyHelper: HierarchyHelper = _
  @transient private var cfStatusMapPs: PreparedStatement = _
  @transient private var hierarchyCache: DataCache = _
  @transient private var objectMapper: com.fasterxml.jackson.databind.ObjectMapper = _
  private val httpUtil = new HttpUtil
  private val clEnrollEndpoint = config.lmsBasePath + config.clEnrollRoute
  private val courseEnrollEndpoint = config.lmsBasePath + config.courseEnrollRoute
  private val redisEnabled = true
  private val optionalCache = scala.collection.mutable.Map[String, Set[String]]()
  @transient private var userCourseStatusPs: PreparedStatement = _
  @transient private var updateOptionalPs: PreparedStatement = _
  @transient private var assessmentQuestionPs: PreparedStatement = _
  @transient private var assessmentContentListPs: PreparedStatement = _

  private val searchUrl = config.searchBasePath + "/v3/search"
  private val OE_KEY_PREFIX = "oe:"

  // Helper: ES fallback to resolve CF Id by base batch id
  private def resolveCfIdFromES(baseBatch: String): String = {
    if (baseBatch == null || baseBatch.isEmpty) return ""
    val url = s"${config.esBasePath}/${config.esActivityBatchIndex}/_search"
    val body = s"""{
                   |  "_source": ["activityId", "activityType"],
                   |  "query": { "term": { "batchId.raw": "${baseBatch}" } }
                   |}""".stripMargin
    try {
      logger.info(s"ESLookup.Request baseBatch=$baseBatch url=$url body=$body")
      val resp = httpUtil.post(url, body, Map("Content-Type" -> "application/json"))
      logger.info(s"ESLookup.Response baseBatch=$baseBatch status=${resp.status} body=${resp.body}")
      val cfId = extractCfIdFromESResponse(resp.body)
      if (cfId.nonEmpty) logger.info(s"ESLookup.Success baseBatch=$baseBatch cfId=$cfId") else logger.warn(s"ESLookup.NotFound baseBatch=$baseBatch")
      cfId
    } catch {
      case ex: Exception =>
        logger.warn(s"ESLookup.Failed baseBatch=$baseBatch", ex)
        ""
    }
  }

  // Helper: parse ES search response and return CF Id when activityType is Competency Framework
  private def extractCfIdFromESResponse(jsonStr: String): String = {
    try {
      val root = objectMapper.readTree(jsonStr)
      val hits = Option(root.get("hits")).flatMap(h => Option(h.get("hits"))).orNull
      if (hits != null && hits.size() > 0) {
        val first = hits.get(0)
        val source = Option(first.get("_source")).orNull
        if (source != null) {
          val at = Option(source.get("activityType")).map(_.asText()).getOrElse("")
          val id = Option(source.get("activityId")).map(_.asText()).getOrElse("")
          if (id.nonEmpty && at.equalsIgnoreCase("Competency Framework")) return id
        }
      }
      ""
    } catch {
      case ex: Exception =>
        logger.warn("ESLookup.ParseFailed", ex)
        ""
    }
  }

  private def mergeAndPersistOptional(userId: String, cfId: String, baseBatch: String, newOptionals: Set[String]): Set[String] = {
    if (newOptionals == null || newOptionals.isEmpty) return optionalCache.getOrElse(optKey(userId, cfId, baseBatch), Set.empty)
    val key = optKey(userId, cfId, baseBatch)
    val existing = optionalCache.getOrElse(key, {
      val (_, opt) = fetchCfStatusAndOptional(userId, cfId, baseBatch)
      opt
    })
    val merged = existing ++ newOptionals
    if (merged != existing) {
      try {
        val encoded: java.util.List[String] = new java.util.ArrayList[String](
          merged.toList.map(cid => batchId(baseBatch, cid)).asJava
        )
        logger.info(s"CQLExecute query='${updateOptionalPs.getQueryString}' values=[${merged.mkString("|")},$userId,$cfId,$baseBatch]")
        val rs = cassandraUtil.session.execute(updateOptionalPs.bind(encoded, userId, cfId, baseBatch))
        val applied = try { rs.wasApplied() } catch { case _: Throwable => true }
        if (applied) {
          optionalCache.put(key, merged)
          logger.info(s"OptionalPersisted user=$userId cf=$cfId baseBatch=$baseBatch count=${merged.size} list=${merged.mkString(",")}")
        } else {
          logger.warn(s"OptionalUpdateNotApplied (row missing) user=$userId cf=$cfId baseBatch=$baseBatch skippingCacheUpdate")
        }
      } catch { case ex: Exception => logger.error(s"OptionalPersistFailed user=$userId cf=$cfId baseBatch=$baseBatch list=${merged.mkString(",")}", ex) }
    }
    merged
  }

  private def batchId(base: String, id: String) = EnrollmentApiUtil.generateBatchId(base, id)
  private def enrollActivity(activityType: String, activityId: String, baseBatch: String, users: List[String], note: String): Unit = {
    if (users != null && users.nonEmpty) {
      val bId = batchId(baseBatch, activityId)
      logger.info(s"EnrollAPIRequest activityType=$activityType activityId=$activityId batchId=$bId baseBatch=$baseBatch users=${users.mkString(",")} note=$note")
      EnrollmentApiUtil.enrollActivity(httpUtil, activityType, activityId, bId, users, note, clEnrollEndpoint, courseEnrollEndpoint)
    } else {
      logger.info(s"EnrollAPISkipEmptyUsers activityType=$activityType activityId=$activityId baseBatch=$baseBatch note=$note")
    }
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    hierarchyHelper = new HierarchyHelper(cassandraUtil, config.dbKeyspace, config.dbTable)
    objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
    cfStatusMapPs = cassandraUtil.session.prepare(s"select statusmap, optional_batches from ${config.sbCollectionKeyspace}.${config.sbCollectionTable} where userid=? and activityid=? and activitytype='Competency Framework' and batchid=?")
    userCourseStatusPs = cassandraUtil.session.prepare(s"select status from ${config.userEnrollKeyspace}.${config.userEnrollTable} where userid=? and courseid=?")
    updateOptionalPs = cassandraUtil.session.prepare(s"update ${config.sbCollectionKeyspace}.${config.sbCollectionTable} set optional_batches=? where userid=? and activityid=? and activitytype='Competency Framework' and batchid=? IF EXISTS")
    assessmentQuestionPs = cassandraUtil.session.prepare(s"select question, last_attempted_on, updated_on, attempt_id from ${config.assessmentAggKeyspace}.${config.assessmentAggTable} where course_id=? and batch_id=? and user_id=? and content_id=?")
    assessmentContentListPs = cassandraUtil.session.prepare(s"select content_id from ${config.assessmentAggKeyspace}.${config.assessmentAggTable} where course_id=? and batch_id=? and user_id=?")
    if (redisEnabled) {
      try {
        hierarchyCache = new DataCache(config, new RedisConnect(config.cfHierarchyRedisHost, config.cfHierarchyRedisPort, config), config.cfHierarchyRedisDb, Nil)
        hierarchyCache.init()
        logger.info(s"Hierarchy DataCache initialised db=${config.cfHierarchyRedisDb}")
      } catch { case ex: Exception => logger.warn("Hierarchy DataCache init failed; proceeding without cache", ex) }
    }
    logger.info("UserEnrollmentFunction opened")
  }

  override def close(): Unit = {
    logger.info("UserEnrollmentFunction closing")
    try { if (hierarchyCache != null) hierarchyCache.close() } catch { case ex: Exception => logger.warn("Hierarchy cache close failed", ex) }
    try { if (cassandraUtil != null) cassandraUtil.close() } catch { case ex: Exception => logger.warn("Cassandra close failed", ex) }
    super.close()
  }

  override def metricsList(): List[String] = List(config.totalEventCount, config.failedEventCount, config.processedEventCount)

  private def normalizeBatchId(raw: String): String = {
    if (raw == null) "" else { val idx = raw.indexOf(':'); if (idx >= 0) raw.substring(0, idx) else raw }
  }

  private def resolveCfIdForEvent(event: Event): String = {
    val courseId = Option(event.courseId).getOrElse("")
    val provided = Option(event.activityId).getOrElse("")
    val base = normalizeBatchId(event.batchId)
    logger.info(s"CFIdResolveStart mid=${event.mid()} courseId=$courseId providedActivityId=$provided batchId=${event.batchId} baseBatch=$base")
    if (provided.nonEmpty && provided != courseId) {
      logger.info(s"CFIdResolveFromProvided mid=${event.mid()} cfId=$provided (providedActivityId)")
      return provided
    }
    if (hierarchyCache != null && base.nonEmpty) {
      try {
        BatchMappingUtil.readBatchMapping(hierarchyCache, base) match {
          case Some((id, tpe)) if tpe.equalsIgnoreCase("Competency Framework") && id.nonEmpty =>
            logger.info(s"CFIdResolveFromMapping mid=${event.mid()} base=$base id=$id type=$tpe")
            return id
          case Some((id, tpe)) =>
            logger.info(s"BatchMappingFound base=$base id=$id type=$tpe but expecting Competency Framework; falling back")
          case None =>
            logger.warn(s"CFIdResolveNoMapping mid=${event.mid()} base=$base")
        }
      } catch { case ex: Exception => logger.error(s"CFIdRedisError baseBatch=$base", ex) }
      val esId = resolveCfIdFromES(base)
      if (esId.nonEmpty) return esId
    } else logger.warn(s"CFIdResolveSkipRedis mid=${event.mid()} baseEmptyOrCacheNull base=$base cacheNull=${hierarchyCache==null}")
    logger.warn(s"CFIdResolveFailed mid=${event.mid()} courseId=$courseId batchId=${event.batchId} baseBatch=$base")
    ""
  }

  private sealed trait NextDecision
  private case class NextCourse(id: String, clId: String) extends NextDecision
  private case class NextLevelExam(id: String, clId: String) extends NextDecision
  private case class NextCL(clId: String) extends NextDecision
  private case object CompletedCF extends NextDecision
  private case object NoOp extends NextDecision
  private case class NextEntranceExamCompleted(clId: String) extends NextDecision

  private def optKey(userId: String, cfId: String, batchId: String) = s"$userId|$cfId|$batchId"

  private def fetchCfStatusAndOptional(userId: String, cfId: String, batchId: String): (Map[String, Int], Set[String]) = {
    try {
      logger.info(s"CQLExecute query='${cfStatusMapPs.getQueryString}' values=[$userId,$cfId,$batchId]")
      val rs = withRetry(cassandraUtil.session.execute(cfStatusMapPs.bind(userId, cfId, batchId)))
      val row = rs.one()
      if (row != null) {
        val sm = if (!row.isNull("statusmap")) row.getMap("statusmap", classOf[String], classOf[Integer]).asScala.map { case (k, v) => k -> v.intValue() }.toMap else Map.empty[String, Int]
        val opt = if (!row.isNull("optional_batches")) {
          row.getList("optional_batches", classOf[String]).asScala.map { s =>
            if (s != null) { val idx = s.lastIndexOf(':'); if (idx >= 0 && idx + 1 < s.length) s.substring(idx + 1) else s } else ""
          }.filter(_.nonEmpty).toSet
        } else Set.empty[String]
        (sm, opt)
      } else (Map.empty[String, Int], Set.empty[String])
    } catch { case ex: Exception => logger.error(s"CFStatusOptionalFetch failure user=$userId cf=$cfId batch=$batchId", ex); (Map.empty[String, Int], Set.empty[String]) }
  }

  private def fetchCfStatusMap(userId: String, cfId: String, batchId: String): Map[String, Int] = fetchCfStatusAndOptional(userId, cfId, batchId)._1

  private def computeAndPersistOptionalCourses(cfId: String, baseBatch: String, users: List[String], structures: Map[String, CLStructure]): Map[String, Set[String]] = {
    if (users == null || users.isEmpty) return Map.empty
    val allCourses = structures.values.flatMap(_.courseIds).toSet
    if (allCourses.isEmpty) return Map.empty
    val result = scala.collection.mutable.Map[String, Set[String]]()
    users.foreach { uid =>
      var detectedOptional = Set.empty[String]
      allCourses.foreach { courseId =>
        try {
          logger.info(s"CQLExecute query='${userCourseStatusPs.getQueryString}' values=[$uid,$courseId]")
          val rs = cassandraUtil.session.execute(userCourseStatusPs.bind(uid, courseId))
          var completedFound = false
          val it = rs.iterator()
          while (it.hasNext) {
            val r = it.next()
            if (!r.isNull("status") && r.getInt("status") == 2) completedFound = true
          }
          if (completedFound) detectedOptional += courseId
        } catch { case ex: Exception => logger.warn(s"OptionalDetectQueryFailed user=$uid course=$courseId", ex) }
      }
      val merged = mergeAndPersistOptional(uid, cfId, baseBatch, detectedOptional)
      result.put(uid, merged)
      if (detectedOptional.nonEmpty) logger.info(s"OptionalCoursesDetected user=$uid cf=$cfId detected=${detectedOptional.mkString(",")} mergedCount=${merged.size}")
    }
    result.toMap
  }

  private def isOptional(userId: String, cfId: String, batchId: String, courseId: String): Boolean = {
    val key = optKey(userId, cfId, batchId)
    optionalCache.get(key) match {
      case Some(set) => set.contains(courseId)
      case None =>
        val (_, opt) = fetchCfStatusAndOptional(userId, cfId, batchId)
        optionalCache.put(key, opt)
        opt.contains(courseId)
    }
  }

  private def filterUsersNeedingCL(cfId: String, clId: String, batchId: String, userIds: List[String]): List[String] = {
    val before = Option(userIds).map(_.size).getOrElse(0)
    val remaining = userIds.filter { uid =>
      val cfStatus = fetchCfStatusMap(uid, cfId, batchId)
      !cfStatus.get(clId).contains(2)
    }
    logger.info(s"CLUserFilter cf=$cfId clId=$clId batchId=$batchId total=$before remaining=${remaining.size} skipped=${before - remaining.size}")
    remaining
  }

  private def loadStructure(cfId: String): (List[CLNode], Map[String, CLStructure], String) = {
    val cached = CFCacheUtil.readStructureFromCache(hierarchyCache, cfId)
    if (cached != null) {
      logger.info(s"StructureLoad cacheHit cf=$cfId clCount=${cached._2.size}")
      cached._2.values.foreach { s =>
        logger.info(s"StructureSnapshot cf=$cfId cl=${s.id} entranceExamId=${Option(s.entranceExamId).getOrElse("<none>")} levelExamId=${Option(s.levelExamId).getOrElse("<none>")} courseCount=${s.courseIds.size} courses=${s.courseIds.mkString(",")}")
      }
      warnDuplicateExamIds(cfId, cached._2);
      return cached
    }
    try {
      val h = hierarchyHelper.getHierarchyWithCache(cfId, hierarchyCache)
      if (h == null || h.isEmpty) { logger.warn(s"StructureLoadEmpty cf=$cfId"); return (Nil, Map.empty, "") }
      val (orderedCLs, structures) = CFCacheUtil.extractCLsAndCourses(h, hierarchyHelper)
      logger.info(s"StructureExtract cf=$cfId clCount=${structures.size} orderedCLs=${orderedCLs.map(_.id).mkString(",")}")
      structures.values.foreach { s =>
        logger.info(s"StructureSnapshot cf=$cfId cl=${s.id} entranceExamId=${Option(s.entranceExamId).getOrElse("<none>")} levelExamId=${Option(s.levelExamId).getOrElse("<none>")} courseCount=${s.courseIds.size} courses=${s.courseIds.mkString(",")}")
      }
      val enrollmentType = Option(h.get("enrollmentType")).map(_.toString.trim).getOrElse("")
      CFCacheUtil.writeStructureToCache(hierarchyCache, cfId, orderedCLs, structures, enrollmentType)
      warnDuplicateExamIds(cfId, structures)
      (orderedCLs, structures, enrollmentType)
    } catch {
      case ex: Exception =>
        logger.error(s"HierarchyLoadFailed cfId=$cfId", ex)
        (Nil, Map.empty, "")
    }
  }

  private def computeNext(completedId: String, clStruct: CLStructure, ordered: List[CLNode], structures: Map[String, CLStructure]): NextDecision = {
    val isEntranceExam = clStruct.entranceExamId != null && clStruct.entranceExamId == completedId
    val isInCourseList = clStruct.courseIds.contains(completedId)
    val isLevelExam = clStruct.levelExamId != null && clStruct.levelExamId == completedId
    if (isEntranceExam) return NextEntranceExamCompleted(clStruct.id)
    if (isInCourseList && !isLevelExam) { val idx = clStruct.courseIds.indexOf(completedId); if (idx < clStruct.courseIds.size - 1) return NextCourse(clStruct.courseIds(idx + 1), clStruct.id); if (clStruct.levelExamId != null) return NextLevelExam(clStruct.levelExamId, clStruct.id) }
    val clFinished = isLevelExam || (isInCourseList && clStruct.courseIds.lastOption.contains(completedId) && clStruct.levelExamId == null)
    if (clFinished) {
      val orderedIds = ordered.map(_.id)
      val idx = orderedIds.indexOf(clStruct.id)
      if (idx >= 0) { if (idx < orderedIds.size - 1) return NextCL(orderedIds(idx + 1)) else return CompletedCF }
    }
    NoOp
  }

  private def progression(event: Event): Unit = {
    if (event.courseId == null || event.courseId.isEmpty) return
    val baseBatch = normalizeBatchId(event.batchId)
    logger.info(s"ProgressionStart mid=${event.mid()} action=${event.action} courseId=${event.courseId} batchId=${event.batchId} baseBatch=$baseBatch")
    val resolvedCfId = resolveCfIdForEvent(event)
    if (resolvedCfId.isEmpty) { logger.warn(s"ProgressionAbort mid=${event.mid()} reason=cfIdNotResolved baseBatch=$baseBatch"); return }
    val (orderedCLs, structures, enrollmentType) = loadStructure(resolvedCfId)
    if (orderedCLs.isEmpty) { logger.warn(s"ProgressionAbort mid=${event.mid()} cf=$resolvedCfId reason=noCLsLoaded"); return }
    val originalUsers = event.userIds
    val strategy = strategyFor(enrollmentType, resolvedCfId, baseBatch, None, orderedCLs, structures)
    val courseToCL = structures.values.flatMap { s => var all = s.courseIds; if (s.levelExamId != null) all = all :+ s.levelExamId; if (s.entranceExamId != null) all = all :+ s.entranceExamId; all.map(_ -> s) }.toMap
    val clStructOpt = courseToCL.get(event.courseId)
    if (clStructOpt.isEmpty) { logger.warn(s"ProgressionAbort mid=${event.mid()} cf=$resolvedCfId reason=courseNotUnderAnyCL courseId=${event.courseId}"); return }
    val clStruct = clStructOpt.get
    logger.info(s"ProgressionCheckEntranceExam strategy=${strategy.name} entranceExamId=${Option(clStruct.entranceExamId).getOrElse("null")} courseId=${event.courseId} users=${Option(originalUsers).map(_.size).getOrElse(0)}")
    if (strategy.isInstanceOf[EntranceExamBasedStrategy] && clStruct.entranceExamId != null && event.courseId == clStruct.entranceExamId) {
      logger.info(s"EEOptional.BuildStart cf=$resolvedCfId cl=${clStruct.id} entranceExam=${clStruct.entranceExamId} baseBatch=$baseBatch users=${Option(originalUsers).map(_.mkString(",")).getOrElse("")}")
      deriveEntranceExamOptionalsSimple(resolvedCfId, clStruct, clStruct.entranceExamId, baseBatch, originalUsers)
      logger.info(s"EEOptional.BuildEnd cf=$resolvedCfId cl=${clStruct.id} entranceExam=${clStruct.entranceExamId}")
    }
    val nextAction = computeNext(event.courseId, clStruct, orderedCLs, structures)
    logger.info(s"ProgressionNextDecision mid=${event.mid()} cf=$resolvedCfId cl=${clStruct.id} current=${event.courseId} decision=${nextAction.toString}")
    val cfBatchId = baseBatch
    val users = nextAction match {
      case NextEntranceExamCompleted(_) =>
        logger.info(s"PostEntranceExamTransition cf=$resolvedCfId cl=${clStruct.id} note=Not re-enrolling entrance exam; proceeding to optional/non-optional/level-exam")
        originalUsers
      case _ =>
        val required = if (originalUsers == null) Nil else {
          // Only Progress Based and Entrance Exam Based consider optionality to gate progression
          strategy match {
            case _: ProgressBasedStrategy | _: EntranceExamBasedStrategy => originalUsers.filterNot(u => isOptional(u, resolvedCfId, cfBatchId, event.courseId))
            case _ => originalUsers
          }
        }
        if (required.isEmpty && (strategy.isInstanceOf[ProgressBasedStrategy] || strategy.isInstanceOf[EntranceExamBasedStrategy])) {
          logger.info(s"SkipOptionalCourseCompletion strategy=${strategy.name} course=${event.courseId} cf=$resolvedCfId totalUsers=${Option(originalUsers).map(_.size).getOrElse(0)} message=optional course completion does not advance progression")
          return
        }
        required
    }
    if (enrollmentType == "Progress Based") {
      val optionalCount = Option(originalUsers).getOrElse(Nil).count(u => isOptional(u, resolvedCfId, cfBatchId, event.courseId))
      if (optionalCount > 0) {
        logger.info(s"ProgressOptionalCourseCompleted course=${event.courseId} cf=$resolvedCfId optionalUsers=$optionalCount totalUsers=${Option(originalUsers).map(_.size).getOrElse(0)}")
      }
    }
    def enrollCLIfTransition(targetClId: String): Unit = if (targetClId != null && targetClId.nonEmpty && targetClId != clStruct.id && users != null && users.nonEmpty) enrollActivity("Competency Level", targetClId, baseBatch, users, s"phase=Progression nextCL=$targetClId fromCL=${clStruct.id}")
    nextAction match {
      case NextEntranceExamCompleted(clId) =>
        enrollActivity("Competency Level", clId, baseBatch, users, s"phase=Progression startCLAfterEntranceExam")
        clStruct.courseIds.foreach { cid =>
          val optionalUsers = users.filter(u => isOptional(u, resolvedCfId, cfBatchId, cid))
          if (optionalUsers.nonEmpty) {
            logger.info(s"EEOptional.EnrollOptional course=$cid optionalUsers=${optionalUsers.mkString(",")}")
            enrollActivity("Course", cid, baseBatch, optionalUsers, s"phase=Progression optionalCourseAfterEntranceExam cl=$clId")
          }
        }
        val remainingUsers = users.filter(u => clStruct.courseIds.exists(cid => !isOptional(u, resolvedCfId, cfBatchId, cid)))
        if (remainingUsers.nonEmpty) {
          val nextNonOptional = clStruct.courseIds.find(cid => remainingUsers.exists(u => !isOptional(u, resolvedCfId, cfBatchId, cid)))
          nextNonOptional match {
            case Some(cid) =>
              val eligibleUsers = remainingUsers.filter(u => !isOptional(u, resolvedCfId, cfBatchId, cid))
              logger.info(s"EEOptional.EnrollFirstNonOptional course=$cid eligibleUsers=${eligibleUsers.mkString(",")}")
              if (eligibleUsers.nonEmpty) enrollActivity("Course", cid, baseBatch, eligibleUsers, s"phase=Progression firstNonOptionalAfterEntranceExam cl=$clId")
            case None =>
              logger.info(s"EEOptional.NoNonOptionalLeft cf=$resolvedCfId cl=$clId users=${users.mkString(",")} proceedingToLevelExamIfAny")
              if (clStruct.levelExamId != null && clStruct.levelExamId.nonEmpty) {
                enrollActivity("Course", clStruct.levelExamId, baseBatch, users, s"phase=Progression levelExamAfterEntranceExam cl=$clId allCoursesOptional")
              }
          }
        } else {
          logger.info(s"EEOptional.AllCoursesOptionalForUsers cf=$resolvedCfId cl=$clId users=${users.mkString(",")} proceedingToLevelExamIfAny")
          if (clStruct.levelExamId != null && clStruct.levelExamId.nonEmpty) {
            enrollActivity("Course", clStruct.levelExamId, baseBatch, users, s"phase=Progression levelExamAfterEntranceExam cl=$clId allCoursesOptional")
          }
        }
      case NextCourse(cid, clId) if users != null && users.nonEmpty =>
        val needUsers = strategy.filterUsersForCourse(cid, resolvedCfId, cfBatchId, users)
        if (needUsers.nonEmpty) {
          enrollCLIfTransition(clId)
          // Enroll optional courses in parallel for both Entrance Exam Based and Progress Based strategies
          if (strategy.isInstanceOf[EntranceExamBasedStrategy] || strategy.isInstanceOf[ProgressBasedStrategy]) {
            clStruct.courseIds.filter(_ != cid).foreach { optCid =>
              val optionalUsers = needUsers.filter(u => isOptional(u, resolvedCfId, cfBatchId, optCid))
              if (optionalUsers.nonEmpty) {
                logger.info(s"ParallelOptional.Enroll baseCourse=$cid optionalCourse=$optCid users=${optionalUsers.mkString(",")}")
                enrollActivity("Course", optCid, baseBatch, optionalUsers, s"phase=Progression optionalInParallel baseCourse=$cid cl=$clId")
              }
            }
          }
          enrollActivity("Course", cid, baseBatch, needUsers, s"phase=Progression next=Course cl=$clId from=${event.courseId} strategy=${strategy.name}")
          if (strategy.isInstanceOf[EntranceExamBasedStrategy] && clStruct.levelExamId != null && clStruct.levelExamId.nonEmpty) {
            val optionalUsers = needUsers.filter(u => isOptional(u, resolvedCfId, cfBatchId, cid))
            if (optionalUsers.nonEmpty) {
              enrollActivity("Course", clStruct.levelExamId, baseBatch, optionalUsers, s"phase=Progression entranceExamOptionalAutoLevelExam cl=$clId course=$cid fromEntranceExam strategy=${strategy.name}")
            }
          }
        } else if (strategy.isInstanceOf[ProgressBasedStrategy] || strategy.isInstanceOf[EntranceExamBasedStrategy] || strategy.isInstanceOf[FullEnrollmentStrategy]) advancePastOptional(cid, clStruct, orderedCLs, structures, resolvedCfId, users, baseBatch, enrollmentType)
      case NextLevelExam(eid, clId) => enrollActivity("Course", eid, baseBatch, users, s"phase=Progression next=LevelExam cl=$clId from=${event.courseId} strategy=${strategy.name}")
      case NextCL(nextClId) => if (users != null && users.nonEmpty) startNextCLWithStrategy(strategy, resolvedCfId, baseBatch, nextClId, structures, users)
      case CompletedCF => logger.info(s"ProgressionComplete cf=$resolvedCfId strategy=${strategy.name}")
      case NoOp => logger.info(s"ProgressionNoOp course=${event.courseId} strategy=${strategy.name}")
      case _ =>
    }
  }

  private def fetchAssessmentRow(courseId: String, batchId: String, userId: String, contentId: String, attempts: Int = 3, delayMs: Long = 800): com.datastax.driver.core.Row = {
    val altBatch = normalizeBatchId(batchId)
    var i = 0
    var row: com.datastax.driver.core.Row = null
    while (i < attempts && row == null) {
      try {
        logger.debug(s"CQLExecute try=${i+1}/$attempts query='${assessmentQuestionPs.getQueryString}' values=[$courseId,$batchId,$userId,$contentId]")
        val rs = withRetry(cassandraUtil.session.execute(assessmentQuestionPs.bind(courseId, batchId, userId, contentId)))
        row = rs.one()
        if (row == null && altBatch != null && altBatch.nonEmpty && altBatch != batchId) {
          logger.debug(s"CQLExecute alt try=${i+1}/$attempts query='${assessmentQuestionPs.getQueryString}' values=[$courseId,$altBatch,$userId,$contentId]")
          val rsAlt = withRetry(cassandraUtil.session.execute(assessmentQuestionPs.bind(courseId, altBatch, userId, contentId)))
          row = rsAlt.one()
        }
        if (row == null) {
          logger.debug(s"AssessmentRowNotFound try=${i+1}/$attempts course=$courseId batch=$batchId altBatch=$altBatch user=$userId content=$contentId sleepingMs=$delayMs")
          Thread.sleep(delayMs)
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"AssessmentRowFetchError try=${i+1}/$attempts course=$courseId batch=$batchId user=$userId content=$contentId", ex)
          Thread.sleep(delayMs)
      }
      i += 1
    }
    if (row == null) {
      val providedIds = listAvailableContentIds(courseId, batchId, userId)
      logger.info(s"AssessmentRowMissingFinal course=$courseId batch=$batchId user=$userId probeContentIds=${providedIds.mkString(",")}")
      if (altBatch != null && altBatch.nonEmpty && altBatch != batchId) {
        val altIds = listAvailableContentIds(courseId, altBatch, userId)
        logger.info(s"AssessmentRowMissingFinalAlt course=$courseId altBatch=$altBatch user=$userId probeContentIds=${altIds.mkString(",")}")
      }
    }
    row
  }

  private def contentScorePercent(courseId: String, batchId: String, userId: String, contentId: String): Map[String, Double] = {
    logger.info(s"ScoreCalcStart course=$courseId batch=$batchId user=$userId content=$contentId")
    try {
      val row = fetchAssessmentRow(courseId, batchId, userId, contentId)
      if (row != null && !row.isNull("question")) {
        val entries = extractQuestionEntries(row)
        logger.info(s"ScoreEntriesFound course=$courseId batch=$batchId user=$userId content=$contentId entries=${entries.size}")
        val scores = entries.flatMap { m =>
          val qId = m.get("id").map(_.toString).getOrElse("")
          if (qId.nonEmpty) {
            val scoreD = try { m.get("score").map(_.toString.toDouble).getOrElse(0.0) } catch { case _: Exception => 0.0 }
            val maxD = try { m.get("max_score").map(_.toString.toDouble).getOrElse(0.0) } catch { case _: Exception => 0.0 }
            val pct = if (maxD > 0) (scoreD / maxD) * 100 else 0.0
            logger.debug(s"ScoreCalc qId=$qId rawScore=$scoreD max=$maxD pct=$pct threshold=${config.entranceExamOptionalThreshold}")
            Some(qId -> pct)
          } else None
        }.toMap
        logger.debug(s"ScoreCalcResult course=$courseId content=$contentId scores=${scores.map{case(k,v)=>s"$k:$v"}.mkString(",")}")
        scores
      } else {
        logger.info(s"ScoreNoRow course=$courseId batch=$batchId user=$userId content=$contentId")
        Map.empty[String, Double]
      }
    } catch { case ex: Exception => logger.warn(s"ContentScorePercentFailed course=$courseId batch=$batchId user=$userId content=$contentId", ex); Map.empty[String, Double] }
  }

  private def listAvailableContentIds(courseId: String, batchId: String, userId: String): List[String] = {
    try {
      logger.info(s"CQLExecute query='${assessmentContentListPs.getQueryString}' values=[$courseId,$batchId,$userId]")
      val rs = withRetry(cassandraUtil.session.execute(assessmentContentListPs.bind(courseId, batchId, userId)))
      val ids = rs.all().asScala.map(r => if (!r.isNull("content_id")) r.getString("content_id") else "").filter(_.nonEmpty).toList
      logger.debug(s"PartitionContentIds course=$courseId batch=$batchId user=$userId count=${ids.size} ids=${ids.mkString(",")}")
      ids
    } catch { case ex: Exception => logger.warn(s"PartitionContentIdsFailed course=$courseId batch=$batchId user=$userId", ex); Nil }
  }

  private def extractQuestionEntries(row: com.datastax.driver.core.Row): List[Map[String, Any]] = {
    if (row == null || row.isNull("question")) return Nil
    try {
      val raw = row.getObject("question")
      raw match {
        case l: java.util.List[_] @unchecked =>
          l.asScala.toList.flatMap {
            case m: java.util.Map[_, _] @unchecked =>
              val scalaMap = m.asScala.collect { case (k: Any, v: Any) if k != null => k.toString -> v }.toMap
              Some(scalaMap)
            case s: String =>
              try {
                val json = objectMapper.readTree(s)
                val id = Option(json.get("id")).map(_.asText()).getOrElse("")
                val score = Option(json.get("score")).map(_.asText()).getOrElse(Option(json.get("score")).map(_.toString).getOrElse(""))
                val maxScore = Option(json.get("max_score")).map(_.asText()).getOrElse(Option(json.get("max_score")).map(_.toString).getOrElse(""))
                Some(Map("id" -> id, "score" -> score, "max_score" -> maxScore))
              } catch { case _: Exception => None }
            case udt: com.datastax.driver.core.UDTValue =>
              def getObj(name: String): Option[AnyRef] = try { if (!udt.isNull(name)) Option(udt.getObject(name)) else None } catch { case _: IllegalArgumentException => None }
              def pick(names: List[String]): Option[AnyRef] = names.view.flatMap(getObj).headOption
              val id = pick(List("id", "question_id", "qid", "item_id")).map(_.toString).getOrElse("")
              val score = pick(List("score", "total_score", "obtained_score", "marks", "score_obtained")).map(_.toString).getOrElse("0")
              val maxScore = pick(List("max_score", "maxscore", "total_max_score", "maxMarks", "max_marks")).map(_.toString).getOrElse("0")
              Some(Map("id" -> id, "score" -> score, "max_score" -> maxScore))
            case other =>
              logger.warn(s"UnknownQuestionElementType type=${other.getClass.getName}")
              None
          }
        case other =>
          logger.warn(s"UnexpectedQuestionColumnType type=${other.getClass.getName}")
          Nil
      }
    } catch { case ex: Exception => logger.warn("ExtractQuestionEntriesFailed", ex); Nil }
  }

  private def extractContentIds(root: java.util.Map[String, AnyRef]): List[String] = {
    val result = scala.collection.mutable.ListBuffer[String]()
    def isCollection(n: java.util.Map[String, AnyRef]) = {
      val mt = Option(n.get("mimeType").asInstanceOf[String]).getOrElse("")
      mt.equalsIgnoreCase("application/vnd.ekstep.content-collection")
    }
    def recurse(node: java.util.Map[String, AnyRef]): Unit = {
      val id = Option(node.get("identifier")).map(_.toString).getOrElse("")
      val children = Option(node.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]).getOrElse(java.util.Collections.emptyList())
      if (children.isEmpty && id.nonEmpty && !isCollection(node)) {
        result += id
      } else {
        children.asScala.foreach(recurse)
      }
    }
    if (root != null && !root.isEmpty) recurse(root)
    result.toList
  }

  private def deriveEntranceExamOptionalsSimple(cfId: String, clStruct: CLStructure, entranceExamId: String, baseBatch: String, users: List[String]): Unit = {
    if (users == null || users.isEmpty) return
    val entranceBatchId = batchId(baseBatch, entranceExamId)
    val entranceHierarchy = try { hierarchyHelper.getHierarchyWithCache(entranceExamId, hierarchyCache) } catch { case ex: Exception => logger.warn(s"EntranceHierarchyLoadFailed course=$entranceExamId", ex); null }
    if (entranceHierarchy == null || entranceHierarchy.isEmpty) { logger.warn(s"EEOptional.EntranceHierarchyEmpty course=$entranceExamId cf=$cfId cl=${clStruct.id}"); return }
    val contentIds = extractContentIds(entranceHierarchy)
    logger.debug(s"EEOptional.ContentList entranceExam=$entranceExamId size=${contentIds.size} ids=${contentIds.mkString(",")}")

    val courseOEMap = getCourseOEMap(cfId, clStruct)
    if (courseOEMap.isEmpty) logger.warn(s"EEOptional.CourseOEMapEmpty cf=$cfId cl=${clStruct.id} note=No OEs discovered under CL subtree for its courses")
    logger.debug(s"EEOptional.CourseOEMapping cf=$cfId cl=${clStruct.id} mapping=${courseOEMap.map{case(c,oes)=>s"$c:[${oes.mkString(",")}] (count=${oes.size})"}.mkString("|")}")

    users.foreach { uid =>
      var totalQuestions = 0
      var passedThreshold = 0
      val optionalObservables = scala.collection.mutable.Set[String]()
      val questionOEMap = scala.collection.mutable.Map[String, List[String]]()
      contentIds.foreach { contentId =>
        val scoreMap = contentScorePercent(entranceExamId, entranceBatchId, uid, contentId)
        if (scoreMap.nonEmpty) {
          scoreMap.foreach { case (qId, pct) =>
            totalQuestions += 1
            val oes = getObservableElements(qId)
            questionOEMap.put(qId, oes)
            if (pct >= config.entranceExamOptionalThreshold) {
              passedThreshold += 1
              optionalObservables ++= oes
              logger.debug(s"EEOptional.ThresholdPassed user=$uid qId=$qId pct=$pct oes=${oes.mkString(",")}")
            } else logger.debug(s"EEOptional.ThresholdNotMet user=$uid qId=$qId pct=$pct threshold=${config.entranceExamOptionalThreshold} oes=${oes.mkString(",")}")
          }
        } else logger.info(s"EEOptional.NoScores user=$uid content=$contentId")
      }
      logger.debug(s"EEOptional.QuestionOEMapping user=$uid size=${questionOEMap.size} mapping=${questionOEMap.map{case(q,os)=>s"$q:[${os.mkString(",")}]"}.mkString("|")}")
      logger.info(s"EEOptional.UserSummary user=$uid contentsScanned=${contentIds.size} totalQuestions=$totalQuestions passedThreshold=$passedThreshold optionalObservablesCount=${optionalObservables.size}")

      if (optionalObservables.nonEmpty) {
        clStruct.courseIds.foreach { courseId =>
          val courseOEs = courseOEMap.getOrElse(courseId, Set.empty[String])
          val overlap = courseOEs.intersect(optionalObservables.toSet)
          logger.debug(s"EEOptional.CourseOverlap user=$uid course=$courseId courseOEsCount=${courseOEs.size} overlapCount=${overlap.size} overlap=${overlap.mkString(",")}")
        }
        val candidateCourses = clStruct.courseIds.toSet
        val newlyOptional = courseOEMap.collect { case (courseId, oes) if candidateCourses.contains(courseId) && oes.exists(optionalObservables.contains) => courseId }.toSet
        logger.debug(s"EEOptional.OptionalObservables user=$uid set=${optionalObservables.mkString(",")}")
        logger.info(s"EEOptional.NewlyOptionalCourses user=$uid courses=${newlyOptional.mkString(",")}")
        if (newlyOptional.nonEmpty) {
          mergeAndPersistOptional(uid, cfId, baseBatch, newlyOptional)
        } else {
          logger.info(s"EEOptional.NoOptionalDerived user=$uid reason=NoOverlap optionalObservablesCount=${optionalObservables.size} courseCount=${clStruct.courseIds.size}")
        }
      } else {
        logger.info(s"EEOptional.NoOptionalObservables user=$uid reason=NoQuestionsPassedThreshold contentsScanned=${contentIds.size} totalQuestions=$totalQuestions passedThreshold=$passedThreshold")
      }
    }
  }

  private def getCourseOEMap(cfId: String, clStruct: CFCacheUtil.CLStructure): Map[String, Set[String]] = {
    val courseIds = clStruct.courseIds.toSet
    if (courseIds.isEmpty) return Map.empty
    try {
      val cfHierarchy = hierarchyHelper.getHierarchyWithCache(cfId, hierarchyCache)
      if (cfHierarchy == null || cfHierarchy.isEmpty) return Map.empty

      def childrenOf(node: java.util.Map[String, AnyRef]): List[java.util.Map[String, AnyRef]] =
        Option(node.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]])
          .map(_.asScala.toList).getOrElse(Nil)

      def findCL(node: java.util.Map[String, AnyRef], depth: Int): Option[java.util.Map[String, AnyRef]] = {
        val id = Option(node.get("identifier")).map(_.toString).getOrElse("")
        if (id == clStruct.id) { logger.info(s"CLNodeFound cf=$cfId cl=${clStruct.id} depth=$depth"); return Some(node) }
        childrenOf(node).view.flatMap(ch => findCL(ch, depth + 1)).headOption
      }

      val clNodeOpt = findCL(cfHierarchy, 0)
      if (clNodeOpt.isEmpty) { logger.warn(s"CLNodeNotFound cf=$cfId cl=${clStruct.id}"); return Map.empty }
      val clNode = clNodeOpt.get

      def readOEs(node: java.util.Map[String, AnyRef]): Set[String] = {
        val lc = Option(node.get("targetobservableElementIds").asInstanceOf[java.util.List[String]]).map(_.asScala.toSet).getOrElse(Set.empty[String])
        val cc = Option(node.get("targetObservableElementIds").asInstanceOf[java.util.List[String]]).map(_.asScala.toSet).getOrElse(Set.empty[String])
        val plain = Option(node.get("observableElementIds").asInstanceOf[java.util.List[String]]).map(_.asScala.toSet).getOrElse(Set.empty[String])
        lc ++ cc ++ plain
      }

      def collectAllOEs(node: java.util.Map[String, AnyRef]): Set[String] = {
        val direct = readOEs(node)
        childrenOf(node).foldLeft(direct) { (acc, ch) => acc ++ collectAllOEs(ch) }
      }

      def findNodeById(node: java.util.Map[String, AnyRef], target: String, depth: Int): Option[(java.util.Map[String, AnyRef], Int)] = {
        val id = Option(node.get("identifier")).map(_.toString).getOrElse("")
        if (id == target) Some((node, depth))
        else childrenOf(node).view.flatMap(ch => findNodeById(ch, target, depth + 1)).headOption
      }

      val mapping = scala.collection.mutable.Map[String, Set[String]]()
      courseIds.foreach { cid =>
        findNodeById(clNode, cid, 0) match {
          case Some((courseNode, depth)) =>
            val oes = collectAllOEs(courseNode)
            mapping.put(cid, oes)
            logger.info(s"CourseOEAggregated cf=$cfId cl=${clStruct.id} course=$cid depthFromCL=$depth oesCount=${oes.size} oes=${oes.mkString(",")}")
            if (hierarchyCache != null) {
              val key = s"coe:$cid"
              val value = s"""{"observableElementIds":${oes.map(o => "\""+o+"\"").mkString("[", ",", "]")}}"""
              val ttl = config.redisTtlSeconds
              try hierarchyCache.setWithExpiry(key, value, ttl) catch { case ex: Exception => logger.warn(s"COECacheStoreFailed course=$cid", ex) }
            }
          case None => logger.warn(s"CourseNodeMissing cf=$cfId cl=${clStruct.id} course=$cid")
        }
      }
      val missing = courseIds.diff(mapping.keySet)
      if (missing.nonEmpty) logger.warn(s"CourseOEMissing cf=$cfId cl=${clStruct.id} courseIds=${missing.mkString(",")}")
      mapping.toMap
    } catch { case ex: Exception => logger.warn(s"CourseOEMapBuildFailed cf=$cfId cl=${clStruct.id}", ex); Map.empty }
  }

  private def getObservableElements(questionId: String): List[String] = {
    if (hierarchyCache == null || questionId == null || questionId.isEmpty) return Nil
    val key = s"${OE_KEY_PREFIX}$questionId"
    try {
      val cached = hierarchyCache.getWithRetry(key)
      if (cached != null && cached.contains("observableElementIds")) {
        val oes = cached("observableElementIds").asInstanceOf[java.util.List[String]].asScala.toList
        logger.info(s"OECacheHit qId=$questionId oes=${oes.mkString(",")}")
        return oes
      }
    } catch { case ex: Exception => logger.warn(s"OECacheFetchError qId=$questionId", ex) }
    try {
      val request = s"""{"request":{"limit":1,"filters":{"identifier":"$questionId","status":["Live"]},"fields":["observableElementIds"]}}"""
      logger.info(s"OESearchRequest qId=$questionId body=$request")
      val response = httpUtil.post(searchUrl, request, Map("Content-Type" -> "application/json"))
      logger.info(s"OESearchResponse qId=$questionId status=${response.status} body=${response.body}")
      val json = objectMapper.readTree(response.body)
      val resultNode = Option(json.get("result"))
      val itemsNode = resultNode.flatMap(r => Option(r.get("items")).orElse(Option(r.get("content")))).orNull
      if (itemsNode != null && itemsNode.size() > 0) {
        val oesNode = itemsNode.get(0).get("observableElementIds")
        val oes = if (oesNode != null) oesNode.asScala.map(_.asText()).toList else Nil
        val value = s"""{"observableElementIds":${oes.map(o => "\""+o+"\"").mkString("[", ",", "]")}}"""
        val ttl = config.redisTtlSeconds
        try hierarchyCache.setWithRetry(key, value) catch { case ex: Exception => logger.warn(s"OECacheStoreFailed qId=$questionId", ex) }
        logger.info(s"OESearchStored qId=$questionId oes=${oes.mkString(",")}")
        oes
      } else {
        logger.info(s"OESearchNoItems qId=$questionId")
        Nil
      }
    } catch { case ex: Exception => logger.warn(s"OEApiFailed questionId=$questionId", ex); Nil }
  }

  private def warnDuplicateExamIds(cfId: String, structures: Map[String, CLStructure]): Unit = {
    val exams = structures.values.flatMap(s => List(Option(s.entranceExamId), Option(s.levelExamId)).flatten).toList
    val duplicates = exams.groupBy(identity).collect { case (id, list) if list.size > 1 => id }
    if (duplicates.nonEmpty) {
      logger.warn(s"DuplicateExamIds cf=$cfId duplicates=${duplicates.mkString(",")}")
    }
  }

  private def withRetry[T](op: => T, attempts: Int = 2, delayMs: Long = 200): T = {
    var i = 0
    var last: Throwable = null
    while (i < attempts) {
      try { return op } catch { case ex: Throwable => last = ex; try { Thread.sleep(delayMs) } catch { case _: Throwable => } }
      i += 1
    }
    throw last
  }

  private def startNextCLWithStrategy(strategy: EnrollmentStrategy, cfId: String, baseBatch: String, nextClId: String, structures: Map[String, CLStructure], users: List[String]): Unit = strategy.startCL(cfId, baseBatch, nextClId, structures, users)

  private def advancePastOptional(currentCourseId: String,
                                  clStruct: CLStructure,
                                  orderedCLs: List[CLNode],
                                  structures: Map[String, CLStructure],
                                  cfId: String,
                                  users: List[String],
                                  baseBatch: String,
                                  enrollmentType: String): Unit = {
    if (clStruct == null || users == null || users.isEmpty) return
    val idx = clStruct.courseIds.indexOf(currentCourseId)
    if (idx < 0) return
    var advanced = false
    var i = idx + 1
    while (i < clStruct.courseIds.size && !advanced) {
      val cid = clStruct.courseIds(i)
      val needUsers = users.filterNot(u => isOptional(u, cfId, baseBatch, cid))
      if (needUsers.nonEmpty) {
        enrollActivity("Course", cid, baseBatch, needUsers, s"phase=Progression skipOptional from=$currentCourseId to=$cid")
        advanced = true
      }
      i += 1
    }
    if (!advanced) {
      if (clStruct.levelExamId != null && clStruct.levelExamId.nonEmpty) {
        enrollActivity("Course", clStruct.levelExamId, baseBatch, users, s"phase=Progression levelExamAfterSkippingOptionals from=$currentCourseId")
      } else {
        logger.info(s"AdvancePastOptionalNoFurtherAction cf=$cfId cl=${clStruct.id} from=$currentCourseId enrollmentType=$enrollmentType")
      }
    }
  }

  private sealed trait EnrollmentStrategy {
    def name: String
    def initialEnroll(event: Event, cfId: String, baseBatch: String, orderedCLs: List[CLNode], structures: Map[String, CLStructure]): Unit
    def startCL(cfId: String, baseBatch: String, clId: String, structures: Map[String, CLStructure], users: List[String]): Unit
    def filterUsersForCourse(courseId: String, cfId: String, cfBatchId: String, users: List[String]): List[String] = users
  }

  private def firstCL(ordered: List[CLNode]) = ordered.headOption
  private def firstCLOptStruct(ordered: List[CLNode], structures: Map[String, CLStructure]) = firstCL(ordered).flatMap(cl => structures.get(cl.id))

  // Helper: first non-optional course and eligible users (simplified replacement for removed method)
  private def firstNonOptionalCourseForUsers(cl: CLStructure, optionalByUser: Map[String, Set[String]], targetUsers: List[String]): (Option[String], List[String]) = {
    if (cl == null || cl.courseIds.isEmpty || targetUsers == null || targetUsers.isEmpty) return (None, Nil)
    cl.courseIds.foreach { cid =>
      val eligible = targetUsers.filter(u => !optionalByUser.getOrElse(u, Set.empty).contains(cid))
      if (eligible.nonEmpty) return (Some(cid), eligible)
    }
    (None, Nil)
  }

  private case class FullEnrollmentStrategy() extends EnrollmentStrategy {
    val name = "Full Enrollment"
    def initialEnroll(event: Event, cfId: String, baseBatch: String, orderedCLs: List[CLNode], structures: Map[String, CLStructure]): Unit = {
      val firstStruct = firstCLOptStruct(orderedCLs, structures); if (firstStruct.isEmpty) return
      val cl = firstStruct.get
      val firstId = cl.id
      val targetUsers = filterUsersNeedingCL(cfId, firstId, event.batchId, event.userIds)
      if (targetUsers.nonEmpty) {
        enrollActivity("Competency Level", firstId, baseBatch, targetUsers, s"phase=Initial type=$name")
        // Full Enrollment: always enroll the first course, irrespective of optionality
        cl.courseIds.headOption.foreach(cid => enrollActivity("Course", cid, baseBatch, targetUsers, s"phase=Initial type=$name cl=$firstId firstCourse"))
      }
    }
    def startCL(cfId: String, baseBatch: String, clId: String, structures: Map[String, CLStructure], users: List[String]): Unit = {
      val clStructOpt = structures.get(clId); if (clStructOpt.isEmpty || users == null || users.isEmpty) return
      val clStruct = clStructOpt.get
      enrollActivity("Competency Level", clId, baseBatch, users, s"phase=Progression startCL type=$name")
      // Full Enrollment: always start with the first course, irrespective of optionality
      clStruct.courseIds.headOption.foreach(cid => enrollActivity("Course", cid, baseBatch, users, s"phase=Progression startFirstCourse cl=$clId type=$name"))
    }
  }

  private case class ProgressBasedStrategy(optionalByUser: Map[String, Set[String]]) extends EnrollmentStrategy {
    val name = "Progress Based"
    def initialEnroll(event: Event, cfId: String, baseBatch: String, orderedCLs: List[CLNode], structures: Map[String, CLStructure]): Unit = {
      val firstStruct = firstCLOptStruct(orderedCLs, structures); if (firstStruct.isEmpty) return
      val cl = firstStruct.get
      val firstId = cl.id
      val targetUsers = filterUsersNeedingCL(cfId, firstId, event.batchId, event.userIds)
      if (targetUsers.nonEmpty) {
        enrollActivity("Competency Level", firstId, baseBatch, targetUsers, s"phase=Initial type=$name")
        val allOptional = cl.courseIds.nonEmpty && targetUsers.nonEmpty && cl.courseIds.forall(c => targetUsers.forall(u => optionalByUser.getOrElse(u, Set.empty).contains(c)))
        if (allOptional) {
          cl.courseIds.foreach(c => enrollActivity("Course", c, baseBatch, targetUsers, s"phase=Initial type=$name replayOptional cl=$firstId course=$c"))
          if (cl.levelExamId != null) enrollActivity("Course", cl.levelExamId, baseBatch, targetUsers, s"phase=Initial type=$name levelExamAfterAllOptional cl=$firstId")
        } else {
          cl.courseIds.foreach { c =>
            val optionalUsers = targetUsers.filter(u => optionalByUser.getOrElse(u, Set.empty).contains(c))
            if (optionalUsers.nonEmpty) {
              enrollActivity("Course", c, baseBatch, optionalUsers, s"phase=Initial type=$name optionalParallel cl=$firstId course=$c")
            }
          }
          val (maybeCourse, eligible) = firstNonOptionalCourseForUsers(cl, optionalByUser, targetUsers)
          maybeCourse match {
            case Some(cid) => enrollActivity("Course", cid, baseBatch, eligible, s"phase=Initial type=$name cl=$firstId firstNonOptionalCourse")
            case None => if (cl.levelExamId != null) enrollActivity("Course", cl.levelExamId, baseBatch, targetUsers, s"phase=Initial type=$name cl=$firstId levelExamFallback")
          }
        }
      }
    }
    def startCL(cfId: String, baseBatch: String, clId: String, structures: Map[String, CLStructure], users: List[String]): Unit = {
      val clStructOpt = structures.get(clId); if (clStructOpt.isEmpty || users == null || users.isEmpty) return
      val cl = clStructOpt.get
      enrollActivity("Competency Level", clId, baseBatch, users, s"phase=Progression startCL type=$name")
      val allOptional = cl.courseIds.nonEmpty && users.nonEmpty && cl.courseIds.forall(c => users.forall(u => isOptional(u, cfId, baseBatch, c)))
      if (allOptional) {
        cl.courseIds.foreach(c => enrollActivity("Course", c, baseBatch, users, s"phase=Progression type=$name replayOptional cl=$clId course=$c"))
        if (cl.levelExamId != null) enrollActivity("Course", cl.levelExamId, baseBatch, users, s"phase=Progression type=$name levelExamAfterAllOptional cl=$clId")
      } else {
        // Enroll optional courses in parallel for users who have them optional
        cl.courseIds.foreach { c =>
          val optionalUsers = users.filter(u => isOptional(u, cfId, baseBatch, c))
          if (optionalUsers.nonEmpty) {
            enrollActivity("Course", c, baseBatch, optionalUsers, s"phase=Progression type=$name optionalParallel cl=$clId course=$c")
          }
        }
        // BUGFIX: If the first course in CL is optional for all users, previously we fell back to level exam.
        // Instead, find the first course in order that is non-optional for at least one user and enroll that.
        val nextNonOptionalOpt = cl.courseIds.find(cid => users.exists(u => !isOptional(u, cfId, baseBatch, cid)))
        nextNonOptionalOpt match {
          case Some(cid) =>
            val eligible = users.filter(u => !isOptional(u, cfId, baseBatch, cid))
            if (eligible.nonEmpty) {
              enrollActivity("Course", cid, baseBatch, eligible, s"phase=Progression startFirstNonOptionalCourse cl=$clId type=$name")
            } else if (cl.levelExamId != null) {
              enrollActivity("Course", cl.levelExamId, baseBatch, users, s"phase=Progression levelExamFallback cl=$clId type=$name")
            }
          case None =>
            if (cl.levelExamId != null) {
              enrollActivity("Course", cl.levelExamId, baseBatch, users, s"phase=Progression levelExamAfterAllOptional cl=$clId type=$name")
            }
        }
      }
    }
    override def filterUsersForCourse(courseId: String, cfId: String, cfBatchId: String, users: List[String]): List[String] = users.filterNot(u => isOptional(u, cfId, cfBatchId, courseId))
  }

  private case class EntranceExamBasedStrategy() extends EnrollmentStrategy {
    val name = "Entrance Exam Based"
    def initialEnroll(event: Event, cfId: String, baseBatch: String, orderedCLs: List[CLNode], structures: Map[String, CLStructure]): Unit = {
      logger.info(s"EntranceInitialStart cf=$cfId userCount=${Option(event.userIds).map(_.size).getOrElse(0)}")
      val firstStruct = firstCLOptStruct(orderedCLs, structures); if (firstStruct.isEmpty) { logger.warn(s"EntranceInitialAbort cf=$cfId reason=noFirstCL"); return }
      val cl = firstStruct.get
      logger.info(s"EntranceInitialCL cf=$cfId cl=${cl.id} entranceExamId=${Option(cl.entranceExamId).getOrElse("<none>")} levelExamId=${Option(cl.levelExamId).getOrElse("<none>")} courseCount=${cl.courseIds.size}")
      val firstId = cl.id
      val targetUsers = filterUsersNeedingCL(cfId, firstId, event.batchId, event.userIds)
      logger.info(s"EntranceInitialTargetUsers cf=$cfId cl=$firstId total=${Option(event.userIds).map(_.size).getOrElse(0)} needed=${targetUsers.size} skipped=${Option(event.userIds).map(_.size).getOrElse(0) - targetUsers.size}")
      if (targetUsers.nonEmpty) {
        enrollActivity("Competency Level", firstId, baseBatch, targetUsers, s"phase=Initial type=$name")
        val entranceId = cl.entranceExamId
        if (entranceId != null && entranceId.trim.nonEmpty) {
          logger.info(s"EntranceExamEnrollAttempt cf=$cfId cl=$firstId entranceExamId=$entranceId users=${targetUsers.mkString(",")}")
          enrollActivity("Course", entranceId, baseBatch, targetUsers, s"phase=Initial type=$name entranceExam cl=$firstId")
        } else {
          logger.warn(s"EntranceExamIdMissing cf=$cfId cl=$firstId; skipping entrance exam enroll")
        }
      } else {
        logger.warn(s"EntranceInitialNoUsers cf=$cfId cl=$firstId reason=allUsersAlreadyCLEnrolled")
      }
    }
    def startCL(cfId: String, baseBatch: String, clId: String, structures: Map[String, CLStructure], users: List[String]): Unit = {
      logger.info(s"EntranceStartCL cf=$cfId cl=$clId userCount=${Option(users).map(_.size).getOrElse(0)}")
      val clStructOpt = structures.get(clId); if (clStructOpt.isEmpty) { logger.warn(s"EntranceStartCLAbort cf=$cfId cl=$clId reason=missingCLStruct"); return }
      val cl = clStructOpt.get
      if (users == null || users.isEmpty) { logger.warn(s"EntranceStartCLAbort cf=$cfId cl=$clId reason=noUsers"); return }
      enrollActivity("Competency Level", clId, baseBatch, users, s"phase=Progression startCL type=$name")
      val entranceId = cl.entranceExamId
      if (entranceId != null && entranceId.trim.nonEmpty) {
        logger.info(s"EntranceExamProgressionEnrollAttempt cf=$cfId cl=$clId entranceExamId=$entranceId users=${users.mkString(",")}")
        enrollActivity("Course", entranceId, baseBatch, users, s"phase=Progression startEntranceExam cl=$clId type=$name")
      } else {
        logger.warn(s"EntranceExamIdMissing cf=$cfId cl=$clId; skipping entrance exam enroll")
      }
    }
    override def filterUsersForCourse(courseId: String, cfId: String, cfBatchId: String, users: List[String]): List[String] = users.filterNot(u => isOptional(u, cfId, cfBatchId, courseId))
  }

  private def strategyFor(enrollmentType: String, cfId: String, baseBatch: String, event: Option[Event], orderedCLs: List[CLNode], structures: Map[String, CLStructure]): EnrollmentStrategy = {
    val et = Option(enrollmentType).map(_.trim).getOrElse("")
    logger.info(s"StrategySelect cf=$cfId enrollmentType='${et}' clCount=${orderedCLs.size} baseBatch=$baseBatch eventCourse=${event.flatMap(e => Option(e.courseId)).getOrElse("<none>")}")
    val strat = et match {
      case "Progress Based" => ProgressBasedStrategy(event.map(e => computeAndPersistOptionalCourses(cfId, baseBatch, e.userIds, structures)).getOrElse(Map.empty))
      case "Entrance Exam Based" => EntranceExamBasedStrategy()
      case _ => FullEnrollmentStrategy()
    }
    logger.info(s"StrategyChosen cf=$cfId type='${strat.name}'")
    strat
  }

  private def processInitial(event: Event, cfId: String): Unit = {
    logger.info(s"InitialProcessStart mid=${event.mid()} cf=$cfId userCount=${Option(event.userIds).map(_.size).getOrElse(0)} users=${Option(event.userIds).map(_.mkString(",")).getOrElse("")} batchId=${event.batchId}")
    val (orderedCLs, structures, enrollmentType) = loadStructure(cfId)
    if (orderedCLs.isEmpty) { logger.warn(s"InitialProcessAbort cf=$cfId reason=noCLs"); return }
    val baseBatch = normalizeBatchId(event.batchId)
    // Do not store batch mappings here; BatchUpdaterFunction handles mapping during creation time
    logger.info(s"InitialEnrollment cf=$cfId batch=${event.batchId} normalizedBatch=$baseBatch enrollmentType='$enrollmentType' totalCLs=${orderedCLs.size}")
    val strategy = strategyFor(enrollmentType, cfId, baseBatch, Some(event), orderedCLs, structures)
    orderedCLs.headOption.foreach(h => logger.info(s"InitialFirstCL cf=$cfId cl=${h.id} entranceExamId=${structures.get(h.id).flatMap(s => Option(s.entranceExamId)).getOrElse("<none>")} levelExamId=${structures.get(h.id).flatMap(s => Option(s.levelExamId)).getOrElse("<none>")}"))
    strategy.initialEnroll(event, cfId, baseBatch, orderedCLs, structures)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    logger.info(s"UserEnrollmentFunction processing event: ${event.mid()}, action: ${event.action}, courseId: ${event.courseId}, activityId: ${event.activityId}, batchId: ${event.batchId}")
    try {
      if (event.courseId == null || event.courseId.isEmpty) {
        processInitial(event, event.activityId)
      } else {
        progression(event)
      }
      metrics.incCounter(config.processedEventCount)
    } catch {
      case ex: Exception =>
        logger.error(s"UserEnrollmentFunction processing failed for event: ${event.mid()}", ex)
        metrics.incCounter(config.failedEventCount)
    }
  }
}
