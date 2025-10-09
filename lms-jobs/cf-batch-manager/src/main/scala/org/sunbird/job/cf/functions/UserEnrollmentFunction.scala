package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.cf.util.{CFCacheUtil, EnrollmentApiUtil, HierarchyHelper}
import org.sunbird.job.cf.util.CFCacheUtil.{CLNode, CLStructure}
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.cache.{DataCache, RedisConnect}
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

  // Cache key prefixes
  private val OE_KEY_PREFIX = "oe:"
  private val BCF_KEY_PREFIX = "bcf:"

  // API endpoints
  private val searchUrl = config.searchBasePath + "/v3/search"

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
    cfStatusMapPs = cassandraUtil.session.prepare(s"select statusmap, optional_collection from ${config.sbCollectionKeyspace}.${config.sbCollectionTable} where userid=? and activityid=? and activitytype='Competency Framework' and batchid=?")
    userCourseStatusPs = cassandraUtil.session.prepare(s"select status from ${config.userEnrollKeyspace}.${config.userEnrollTable} where userid=? and courseid=?")
    updateOptionalPs = cassandraUtil.session.prepare(s"update ${config.sbCollectionKeyspace}.${config.sbCollectionTable} set optional_collection=? where userid=? and activityid=? and activitytype='Competency Framework' and batchid=?")
    assessmentQuestionPs = cassandraUtil.session.prepare(s"select question, last_attempted_on, updated_on, attempt_id from ${config.assessmentAggKeyspace}.${config.assessmentAggTable} where course_id=? and batch_id=? and user_id=? and content_id=?")
    assessmentContentListPs = cassandraUtil.session.prepare(s"select content_id from ${config.assessmentAggKeyspace}.${config.assessmentAggTable} where course_id=? and batch_id=? and user_id=?")
    if (redisEnabled) {
      try {
        hierarchyCache = new DataCache(config, new RedisConnect(config), config.cfHierarchyRedisDb, Nil)
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

  private def storeCfBatchMapping(baseBatch: String, cfId: String): Unit = {
    if (hierarchyCache != null && baseBatch.nonEmpty && cfId.nonEmpty) {
      val key = s"$BCF_KEY_PREFIX$baseBatch"
      val value = "{\"cfId\":\"" + cfId + "\"}"
      try {
        hierarchyCache.setWithRetry(key, value)
      } catch {
        case ex: Exception => logger.warn(s"CFBatchMappingStoreFailed base=$baseBatch cfId=$cfId", ex)
      }
    }
  }

  private def resolveCfIdForEvent(event: Event): String = {
    val courseId = Option(event.courseId).getOrElse("")
    val provided = Option(event.activityId).getOrElse("")
    if (provided.nonEmpty && provided != courseId) return provided
    val base = normalizeBatchId(event.batchId)
    if (hierarchyCache != null && base.nonEmpty) {
      val key = s"$BCF_KEY_PREFIX$base"
      try {
        val map = hierarchyCache.getWithRetry(key)
        val cf = map.get("cfId").orElse(map.get("cfid")).map(_.asInstanceOf[String]).getOrElse("")
        if (cf.nonEmpty) return cf
      } catch { case ex: Exception => logger.error(s"CFIdRedisError baseBatch=$base", ex) }
    }
    ""
  }

  private sealed trait NextDecision
  private case class NextCourse(id: String, clId: String) extends NextDecision
  private case class NextLevelExam(id: String, clId: String) extends NextDecision
  private case class NextEntranceExam(id: String, clId: String) extends NextDecision
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
        val opt = if (!row.isNull("optional_collection")) row.getList("optional_collection", classOf[String]).asScala.toSet else Set.empty[String]
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
      var optional = Set.empty[String]
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
          if (completedFound) optional += courseId
        } catch { case ex: Exception => logger.warn(s"OptionalDetectQueryFailed user=$uid course=$courseId", ex) }
      }
      if (optional.nonEmpty) {
        try { logger.info(s"CQLExecute query='${updateOptionalPs.getQueryString}' values=[${optional.mkString("|")},$uid,$cfId,$baseBatch]"); cassandraUtil.session.execute(updateOptionalPs.bind(new java.util.ArrayList[String](optional.asJava), uid, cfId, baseBatch)) } catch { case ex: Exception => logger.error(s"OptionalPersistFailed user=$uid cf=$cfId optional=${optional.mkString(",")}", ex) }
        optionalCache.put(optKey(uid, cfId, baseBatch), optional)
        logger.info(s"OptionalCoursesDetected user=$uid cf=$cfId count=${optional.size} list=${optional.mkString(",")}")
      }
      result.put(uid, optional)
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

  @deprecated private def filterUsersNeedingCL(clId: String, event: Event): List[String] = filterUsersNeedingCL(event.activityId, clId, event.batchId, event.userIds)

  private def loadStructure(cfId: String): (List[CLNode], Map[String, CLStructure], String) = {
    val cached = CFCacheUtil.readStructureFromCache(hierarchyCache, cfId)
    if (cached != null) { warnDuplicateExamIds(cfId, cached._2); return cached }
    try {
      val h = hierarchyHelper.getHierarchy(cfId)
      if (h == null || h.isEmpty) return (Nil, Map.empty, "")
      val (orderedCLs, structures) = CFCacheUtil.extractCLsAndCourses(h, hierarchyHelper)
      val enrollmentType = h.getOrDefault("enrollmentType", "").asInstanceOf[String]
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

  private def enrollStartOfCL(cfId: String, baseBatch: String, clId: String, structures: Map[String, CLStructure], enrollmentType: String, users: List[String]): Unit = {
    val clStructOpt = structures.get(clId)
    if (clStructOpt.isEmpty) return
    val clStruct = clStructOpt.get
    val cfBatchId = batchId(baseBatch, cfId)
    val filteredUsers = users
    enrollActivity("Competency Level", clId, baseBatch, filteredUsers, s"phase=Progression startCL=$clId")
    enrollmentType match {
      case "Full Enrollment" =>
        if (clStruct.courseIds.nonEmpty) {
          val first = clStruct.courseIds.head
          enrollActivity("Course", first, baseBatch, filteredUsers, s"phase=Progression startFirstCourse cl=$clId")
        }
      case "Progress Based" =>
        val allOptional = clStruct.courseIds.nonEmpty && filteredUsers.nonEmpty && clStruct.courseIds.forall(c => filteredUsers.forall(u => isOptional(u, cfId, cfBatchId, c)))
        if (allOptional) {
          clStruct.courseIds.foreach(c => enrollActivity("Course", c, baseBatch, filteredUsers, s"phase=Progression replayOptional cl=$clId course=$c"))
          if (clStruct.levelExamId != null) enrollActivity("Course", clStruct.levelExamId, baseBatch, filteredUsers, s"phase=Progression levelExamAfterAllOptional cl=$clId")
        } else if (clStruct.courseIds.nonEmpty) {
          val first = clStruct.courseIds.find(c => filteredUsers.exists(u => !isOptional(u, cfId, cfBatchId, c)))
          first match {
            case Some(f) =>
              val needUsers = filteredUsers.filter(u => !isOptional(u, cfId, cfBatchId, f))
              if (needUsers.nonEmpty) enrollActivity("Course", f, baseBatch, needUsers, s"phase=Progression startFirstNonOptional cl=$clId")
              else if (clStruct.levelExamId != null) enrollActivity("Course", clStruct.levelExamId, baseBatch, filteredUsers, s"phase=Progression levelExamFallback cl=$clId")
            case None => if (clStruct.levelExamId != null) enrollActivity("Course", clStruct.levelExamId, baseBatch, filteredUsers, s"phase=Progression levelExamFallbackAllOptional cl=$clId")
          }
        } else if (clStruct.levelExamId != null) {
          enrollActivity("Course", clStruct.levelExamId, baseBatch, filteredUsers, s"phase=Progression levelExamOnly cl=$clId")
        }
      case "Entrance Exam Based" =>
        if (clStruct.entranceExamId != null) enrollActivity("Course", clStruct.entranceExamId, baseBatch, filteredUsers, s"phase=Progression startEntranceExam cl=$clId")
      case _ =>
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

  private case class FullEnrollmentStrategy() extends EnrollmentStrategy {
    val name = "Full Enrollment"
    def initialEnroll(event: Event, cfId: String, baseBatch: String, orderedCLs: List[CLNode], structures: Map[String, CLStructure]): Unit = {
      val firstStruct = firstCLOptStruct(orderedCLs, structures); if (firstStruct.isEmpty) return
      val firstId = firstStruct.get.id
      val targetUsers = filterUsersNeedingCL(cfId, firstId, event.batchId, event.userIds)
      if (targetUsers.nonEmpty) {
        enrollActivity("Competency Level", firstId, baseBatch, targetUsers, s"phase=Initial type=$name")
        firstStruct.get.courseIds.headOption.foreach(cid => enrollActivity("Course", cid, baseBatch, targetUsers, s"phase=Initial type=$name cl=$firstId firstCourse"))
      }
    }
    def startCL(cfId: String, baseBatch: String, clId: String, structures: Map[String, CLStructure], users: List[String]): Unit = {
      val clStructOpt = structures.get(clId); if (clStructOpt.isEmpty || users == null || users.isEmpty) return
      val clStruct = clStructOpt.get
      enrollActivity("Competency Level", clId, baseBatch, users, s"phase=Progression startCL type=$name")
      val allOptional = clStruct.courseIds.nonEmpty && users.nonEmpty && clStruct.courseIds.forall(c => users.forall(u => isOptional(u, cfId, baseBatch, c)))
      if (allOptional) {
        if (clStruct.levelExamId != null) enrollActivity("Course", clStruct.levelExamId, baseBatch, users, s"phase=Progression type=$name levelExamAfterAllOptional cl=$clId")
      } else {
        clStruct.courseIds.headOption match {
          case Some(first) =>
            val eligible = users.filterNot(u => isOptional(u, cfId, baseBatch, first))
            if (eligible.nonEmpty) enrollActivity("Course", first, baseBatch, eligible, s"phase=Progression startFirstCourse cl=$clId type=$name")
            else if (clStruct.levelExamId != null) enrollActivity("Course", clStruct.levelExamId, baseBatch, users, s"phase=Progression levelExamFallback cl=$clId type=$name")
          case None => if (clStruct.levelExamId != null) enrollActivity("Course", clStruct.levelExamId, baseBatch, users, s"phase=Progression levelExamOnly cl=$clId type=$name")
        }
      }
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
          val (maybeCourse, eligible) = firstNonOptionalCourseForUsersProgressBased(cl, orderedCLs, structures, optionalByUser, targetUsers)
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
        cl.courseIds.headOption match {
          case Some(first) =>
            val eligible = users.filterNot(u => isOptional(u, cfId, baseBatch, first))
            if (eligible.nonEmpty) enrollActivity("Course", first, baseBatch, eligible, s"phase=Progression startFirstCourse cl=$clId type=$name")
            else if (cl.levelExamId != null) enrollActivity("Course", cl.levelExamId, baseBatch, users, s"phase=Progression levelExamFallback cl=$clId type=$name")
          case None => if (cl.levelExamId != null) enrollActivity("Course", cl.levelExamId, baseBatch, users, s"phase=Progression levelExamOnly cl=$clId type=$name")
        }
      }
    }
    override def filterUsersForCourse(courseId: String, cfId: String, cfBatchId: String, users: List[String]): List[String] = users.filterNot(u => isOptional(u, cfId, cfBatchId, courseId))
  }

  private case class EntranceExamBasedStrategy() extends EnrollmentStrategy {
    val name = "Entrance Exam Based"
    def initialEnroll(event: Event, cfId: String, baseBatch: String, orderedCLs: List[CLNode], structures: Map[String, CLStructure]): Unit = {
      val firstStruct = firstCLOptStruct(orderedCLs, structures); if (firstStruct.isEmpty) return
      val firstId = firstStruct.get.id
      val targetUsers = filterUsersNeedingCL(cfId, firstId, event.batchId, event.userIds)
      if (targetUsers.nonEmpty) {
        enrollActivity("Competency Level", firstId, baseBatch, targetUsers, s"phase=Initial type=$name")
        val cl = firstStruct.get
        if (cl.entranceExamId != null) enrollActivity("Course", cl.entranceExamId, baseBatch, targetUsers, s"phase=Initial type=$name entranceExam cl=$firstId")
      }
    }
    def startCL(cfId: String, baseBatch: String, clId: String, structures: Map[String, CLStructure], users: List[String]): Unit = {
      val clStructOpt = structures.get(clId); if (clStructOpt.isEmpty || users == null || users.isEmpty) return
      val cl = clStructOpt.get
      enrollActivity("Competency Level", clId, baseBatch, users, s"phase=Progression startCL type=$name")
      if (cl.entranceExamId != null) enrollActivity("Course", cl.entranceExamId, baseBatch, users, s"phase=Progression startEntranceExam cl=$clId type=$name")
    }
    override def filterUsersForCourse(courseId: String, cfId: String, cfBatchId: String, users: List[String]): List[String] = users.filterNot(u => isOptional(u, cfId, cfBatchId, courseId))
  }

  private def strategyFor(enrollmentType: String, cfId: String, baseBatch: String, event: Option[Event], orderedCLs: List[CLNode], structures: Map[String, CLStructure]): EnrollmentStrategy = {
    enrollmentType match {
      case "Progress Based" => ProgressBasedStrategy(event.map(e => computeAndPersistOptionalCourses(cfId, baseBatch, e.userIds, structures)).getOrElse(Map.empty))
      case "Entrance Exam Based" => EntranceExamBasedStrategy()
      case _ => FullEnrollmentStrategy()
    }
  }

  private def processInitial(event: Event, cfId: String): Unit = {
    val (orderedCLs, structures, enrollmentType) = loadStructure(cfId)
    if (orderedCLs.isEmpty) return
    val baseBatch = normalizeBatchId(event.batchId)
    storeCfBatchMapping(baseBatch, cfId)
    logger.info(s"InitialEnrollment cf=$cfId batch=${event.batchId} normalizedBatch=$baseBatch enrollmentType=$enrollmentType totalCLs=${orderedCLs.size}")
    val strategy = strategyFor(enrollmentType, cfId, baseBatch, Some(event), orderedCLs, structures)
    strategy.initialEnroll(event, cfId, baseBatch, orderedCLs, structures)
  }

  private def startNextCLWithStrategy(strategy: EnrollmentStrategy, cfId: String, baseBatch: String, nextClId: String, structures: Map[String, CLStructure], users: List[String]): Unit = strategy.startCL(cfId, baseBatch, nextClId, structures, users)

  private def progression(event: Event): Unit = {
    if (event.courseId == null || event.courseId.isEmpty) return
    val resolvedCfId = resolveCfIdForEvent(event)
    if (resolvedCfId.isEmpty) return
    val (orderedCLs, structures, enrollmentType) = loadStructure(resolvedCfId)
    if (orderedCLs.isEmpty) return
    val baseBatch = normalizeBatchId(event.batchId)
    val originalUsers = event.userIds // define early so it can be used for optional derivation
    val strategy = strategyFor(enrollmentType, resolvedCfId, baseBatch, None, orderedCLs, structures)
    val courseToCL = structures.values.flatMap { s => var all = s.courseIds; if (s.levelExamId != null) all = all :+ s.levelExamId; if (s.entranceExamId != null) all = all :+ s.entranceExamId; all.map(_ -> s) }.toMap
    val clStructOpt = courseToCL.get(event.courseId)
    if (clStructOpt.isEmpty) return
    val clStruct = clStructOpt.get
    // On entrance exam completion, build mappings & derive optionals before computing next step
    logger.info(s"ProgressionCheckEntranceExam strategy=${strategy.name} entranceExamId=${Option(clStruct.entranceExamId).getOrElse("null")} courseId=${event.courseId} users=${Option(originalUsers).map(_.size).getOrElse(0)}")
    if (strategy.isInstanceOf[EntranceExamBasedStrategy] && clStruct.entranceExamId != null && event.courseId == clStruct.entranceExamId) {
      logger.info(s"EEOptional.BuildStart cf=$resolvedCfId cl=${clStruct.id} entranceExam=${clStruct.entranceExamId} baseBatch=$baseBatch users=${Option(originalUsers).map(_.mkString(",")).getOrElse("")}")
      deriveEntranceExamOptionalsSimple(resolvedCfId, clStruct, clStruct.entranceExamId, baseBatch, originalUsers)
      logger.info(s"EEOptional.BuildEnd cf=$resolvedCfId cl=${clStruct.id} entranceExam=${clStruct.entranceExamId}")
    }
    val nextAction = computeNext(event.courseId, clStruct, orderedCLs, structures)
    val cfBatchId = baseBatch
    val users = nextAction match {
      case NextEntranceExamCompleted(_) =>
        logger.info(s"PostEntranceExamTransition cf=$resolvedCfId cl=${clStruct.id} note=Not re-enrolling entrance exam; proceeding to optional/non-optional/level-exam")
        originalUsers
      case _ =>
        val required = if (originalUsers == null) Nil else originalUsers.filterNot(u => isOptional(u, resolvedCfId, cfBatchId, event.courseId))
        if (required.isEmpty) { logger.info(s"SkipOptionalCourseCompletion strategy=${strategy.name} course=${event.courseId} cf=$resolvedCfId totalUsers=${Option(originalUsers).map(_.size).getOrElse(0)} message=optional course completion does not advance progression"); return }
        required
    }
    // Log details for optional course completion in Progress Based
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
        val nonOptionalQueue = scala.collection.mutable.ListBuffer[String]()
        // Enroll optional courses for users who have them optional
        clStruct.courseIds.foreach { cid =>
          val optionalUsers = users.filter(u => isOptional(u, resolvedCfId, cfBatchId, cid))
          if (optionalUsers.nonEmpty) {
            logger.info(s"EEOptional.EnrollOptional course=$cid optionalUsers=${optionalUsers.mkString(",")}")
            enrollActivity("Course", cid, baseBatch, optionalUsers, s"phase=Progression optionalCourseAfterEntranceExam cl=$clId")
          }
        }
        // Determine first non-optional course for remaining users and enroll them
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
          // For Entrance Exam Based: also enroll optional courses (if any) in parallel for those users
          if (strategy.isInstanceOf[EntranceExamBasedStrategy]) {
            clStruct.courseIds.filter(_ != cid).foreach { optCid =>
              val optionalUsers = needUsers.filter(u => isOptional(u, resolvedCfId, cfBatchId, optCid))
              if (optionalUsers.nonEmpty) {
                logger.info(s"EEOptional.EnrollParallelOptional baseCourse=$cid optionalCourse=$optCid users=${optionalUsers.mkString(",")}")
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
        } else if (strategy.isInstanceOf[ProgressBasedStrategy] || strategy.isInstanceOf[EntranceExamBasedStrategy]) advancePastOptional(cid, clStruct, orderedCLs, structures, resolvedCfId, users, baseBatch, enrollmentType)
      case NextLevelExam(eid, clId) => enrollActivity("Course", eid, baseBatch, users, s"phase=Progression next=LevelExam cl=$clId from=${event.courseId} strategy=${strategy.name}")
      case NextEntranceExam(eid, clId) => enrollCLIfTransition(clId); enrollActivity("Course", eid, baseBatch, users, s"phase=Progression next=EntranceExam cl=$clId from=${clStruct.id} strategy=${strategy.name}")
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
        logger.info(s"CQLExecute try=${i+1}/$attempts query='${assessmentQuestionPs.getQueryString}' values=[$courseId,$batchId,$userId,$contentId]")
        val rs = withRetry(cassandraUtil.session.execute(assessmentQuestionPs.bind(courseId, batchId, userId, contentId)))
        row = rs.one()
        if (row == null && altBatch != null && altBatch.nonEmpty && altBatch != batchId) {
          logger.info(s"CQLExecute alt try=${i+1}/$attempts query='${assessmentQuestionPs.getQueryString}' values=[$courseId,$altBatch,$userId,$contentId]")
          val rsAlt = withRetry(cassandraUtil.session.execute(assessmentQuestionPs.bind(courseId, altBatch, userId, contentId)))
          row = rsAlt.one()
        }
        if (row == null) {
          logger.info(s"AssessmentRowNotFound try=${i+1}/$attempts course=$courseId batch=$batchId altBatch=$altBatch user=$userId content=$contentId sleepingMs=$delayMs")
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

  private def getQuestionIdsForContent(courseId: String, batchId: String, userId: String, contentId: String): List[String] = {
    logger.info(s"QFetchStart course=$courseId batch=$batchId user=$userId content=$contentId")
    try {
      val row = fetchAssessmentRow(courseId, batchId, userId, contentId)
      if (row != null && !row.isNull("question")) {
        logger.info(s"QFetchRowFound course=$courseId batch=$batchId user=$userId content=$contentId")
        val entries = extractQuestionEntries(row)
        val qIds = entries.map(m => m.get("id").map(_.toString).getOrElse("")).filter(_.nonEmpty)
        logger.info(s"QFetchIds course=$courseId batch=$batchId user=$userId content=$contentId qIds=${qIds.mkString(",")}")
        qIds
      } else {
        logger.info(s"QFetchNoRow course=$courseId batch=$batchId user=$userId content=$contentId")
        Nil
      }
    } catch { case ex: Exception => logger.warn(s"QuestionIdsFetchFailed course=$courseId batch=$batchId user=$userId content=$contentId", ex); Nil }
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
            logger.info(s"ScoreCalc qId=$qId rawScore=$scoreD max=$maxD pct=$pct threshold=${config.entranceExamOptionalThreshold}")
            Some(qId -> pct)
          } else None
        }.toMap
        logger.info(s"ScoreCalcResult course=$courseId content=$contentId scores=${scores.map{case(k,v)=>s"$k:$v"}.mkString(",")}")
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
      logger.info(s"PartitionContentIds course=$courseId batch=$batchId user=$userId count=${ids.size} ids=${ids.mkString(",")}")
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
              // Handle UDT question elements: pick common field aliases
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
    // Extract all content (leaf) ids from entrance exam hierarchy (course)
    val entranceHierarchy = try { hierarchyHelper.getHierarchy(entranceExamId) } catch { case ex: Exception => logger.warn(s"EntranceHierarchyLoadFailed course=$entranceExamId", ex); null }
    if (entranceHierarchy == null || entranceHierarchy.isEmpty) { logger.warn(s"EEOptional.EntranceHierarchyEmpty course=$entranceExamId cf=$cfId cl=${clStruct.id}"); return }
    val contentIds = extractContentIds(entranceHierarchy)
    logger.info(s"EEOptional.ContentList entranceExam=$entranceExamId size=${contentIds.size} ids=${contentIds.mkString(",")}")

    // Build course -> OE mapping from CF hierarchy's CL subtree
    val courseOEMap = getCourseOEMap(cfId, clStruct)
    if (courseOEMap.isEmpty) logger.warn(s"EEOptional.CourseOEMapEmpty cf=$cfId cl=${clStruct.id} note=No OEs discovered under CL subtree for its courses")
    logger.info(s"EEOptional.CourseOEMapping cf=$cfId cl=${clStruct.id} mapping=${courseOEMap.map{case(c,oes)=>s"$c:[${oes.mkString(",")} ] (count=${oes.size})"}.mkString("|")}")

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
              logger.info(s"EEOptional.ThresholdPassed user=$uid qId=$qId pct=$pct oes=${oes.mkString(",")}")
            } else logger.info(s"EEOptional.ThresholdNotMet user=$uid qId=$qId pct=$pct threshold=${config.entranceExamOptionalThreshold} oes=${oes.mkString(",")}")
          }
        } else logger.info(s"EEOptional.NoScores user=$uid content=$contentId")
      }
      logger.info(s"EEOptional.QuestionOEMapping user=$uid size=${questionOEMap.size} mapping=${questionOEMap.map{case(q,os)=>s"$q:[${os.mkString(",")} ]"}.mkString("|")}")
      logger.info(s"EEOptional.UserSummary user=$uid contentsScanned=${contentIds.size} totalQuestions=$totalQuestions passedThreshold=$passedThreshold optionalObservablesCount=${optionalObservables.size}")

      if (optionalObservables.nonEmpty) {
        // Per-course overlap diagnostics
        clStruct.courseIds.foreach { courseId =>
          val courseOEs = courseOEMap.getOrElse(courseId, Set.empty[String])
          val overlap = courseOEs.intersect(optionalObservables.toSet)
          logger.info(s"EEOptional.CourseOverlap user=$uid course=$courseId courseOEsCount=${courseOEs.size} overlapCount=${overlap.size} overlap=${overlap.mkString(",")}")
        }
        // Determine optional courses: any course whose OE list intersects optionalObservables
        val candidateCourses = clStruct.courseIds.toSet
        val newlyOptional = courseOEMap.collect { case (courseId, oes) if candidateCourses.contains(courseId) && oes.exists(optionalObservables.contains) => courseId }.toSet
        logger.info(s"EEOptional.OptionalObservables user=$uid set=${optionalObservables.mkString(",")}")
        logger.info(s"EEOptional.NewlyOptionalCourses user=$uid courses=${newlyOptional.mkString(",")}")
        if (newlyOptional.nonEmpty) {
          val key = optKey(uid, cfId, baseBatch)
          val merged = optionalCache.getOrElse(key, Set.empty) ++ newlyOptional
          try {
            logger.info(s"CQLExecute query='${updateOptionalPs.getQueryString}' values=[${merged.mkString("|")},$uid,$cfId,$baseBatch]")
            cassandraUtil.session.execute(updateOptionalPs.bind(new java.util.ArrayList[String](merged.asJava), uid, cfId, baseBatch))
            optionalCache.put(key, merged)
            logger.info(s"EEOptional.Persisted cf=$cfId cl=${clStruct.id} user=$uid courses=${merged.mkString(",")}")
          } catch { case ex: Exception => logger.warn(s"EEOptional.PersistFailed cf=$cfId cl=${clStruct.id} user=$uid courses=${newlyOptional.mkString(",")}", ex) }
        } else {
          logger.info(s"EEOptional.NoOptionalDerived user=$uid reason=NoOverlap optionalObservablesCount=${optionalObservables.size} courseCount=${clStruct.courseIds.size}")
        }
      } else {
        logger.info(s"EEOptional.NoOptionalObservables user=$uid reason=NoQuestionsPassedThreshold contentsScanned=${contentIds.size} totalQuestions=$totalQuestions passedThreshold=$passedThreshold")
      }
    }
  }

  // Build mapping courseId -> Set[OE] for courses inside the CL subtree (depth-agnostic search)
  private def getCourseOEMap(cfId: String, clStruct: CLStructure): Map[String, Set[String]] = {
    val courseIds = clStruct.courseIds.toSet
    if (courseIds.isEmpty) return Map.empty
    try {
      val cfHierarchy = hierarchyHelper.getHierarchy(cfId)
      if (cfHierarchy == null || cfHierarchy.isEmpty) return Map.empty

      def childrenOf(node: java.util.Map[String, AnyRef]): List[java.util.Map[String, AnyRef]] =
        Option(node.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]])
          .map(_.asScala.toList).getOrElse(Nil)

      // Find CL node anywhere in CF hierarchy
      def findCL(node: java.util.Map[String, AnyRef], depth: Int): Option[java.util.Map[String, AnyRef]] = {
        val id = Option(node.get("identifier")).map(_.toString).getOrElse("")
        if (id == clStruct.id) { logger.info(s"CLNodeFound cf=$cfId cl=${clStruct.id} depth=$depth"); return Some(node) }
        childrenOf(node).view.flatMap(ch => findCL(ch, depth + 1)).headOption
      }

      val clNodeOpt = findCL(cfHierarchy, 0)
      if (clNodeOpt.isEmpty) { logger.warn(s"CLNodeNotFound cf=$cfId cl=${clStruct.id}"); return Map.empty }
      val clNode = clNodeOpt.get

      // Collect all OEs in subtree of a node
      def collectAllOEs(node: java.util.Map[String, AnyRef]): Set[String] = {
        val direct = Option(node.get("targetobservableElementIds").asInstanceOf[java.util.List[String]])
          .map(_.asScala.toSet).getOrElse(Set.empty[String])
        childrenOf(node).foldLeft(direct) { (acc, ch) => acc ++ collectAllOEs(ch) }
      }

      // For each courseId locate its node under CL subtree and aggregate OEs
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
          case None => logger.warn(s"CourseNodeMissing cf=$cfId cl=${clStruct.id} course=$cid")
        }
      }
      val missing = courseIds.diff(mapping.keySet)
      if (missing.nonEmpty) logger.warn(s"CourseOEMissing cf=$cfId cl=${clStruct.id} courseIds=${missing.mkString(",")}")
      mapping.toMap
    } catch { case ex: Exception => logger.warn(s"CourseOEMapBuildFailed cf=$cfId cl=${clStruct.id}", ex); Map.empty }
  }

  private def firstNonOptionalCourseForUsersProgressBased(cl: CLStructure,
                                                          orderedCLs: List[CLNode],
                                                          structures: Map[String, CLStructure],
                                                          optionalByUser: Map[String, Set[String]],
                                                          targetUsers: List[String]): (Option[String], List[String]) = {
    if (cl == null || cl.courseIds.isEmpty || targetUsers == null || targetUsers.isEmpty) return (None, Nil)
    // Iterate courses in order; pick first where at least one user does NOT have it optional
    cl.courseIds.foreach { cid =>
      val eligible = targetUsers.filter(u => !optionalByUser.getOrElse(u, Set.empty).contains(cid))
      if (eligible.nonEmpty) return (Some(cid), eligible)
    }
    (None, Nil)
  }

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
    // Look ahead for first course that is non-optional for at least one user
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
      // All remaining courses optional; enroll level exam if present
      if (clStruct.levelExamId != null && clStruct.levelExamId.nonEmpty) {
        enrollActivity("Course", clStruct.levelExamId, baseBatch, users, s"phase=Progression levelExamAfterSkippingOptionals from=$currentCourseId")
      } else {
        // Nothing else to do within this CL; next CL handled by normal progression when level exam (or last course) completes
        logger.info(s"AdvancePastOptionalNoFurtherAction cf=$cfId cl=${clStruct.id} from=$currentCourseId enrollmentType=$enrollmentType")
      }
    }
  }

  private def withRetry[T](operation: => T, maxRetries: Int = 3, delayMs: Long = 1000): T = {
    var attempt = 0
    while (attempt < maxRetries) {
      try {
        return operation
      } catch {
        case ex: Exception =>
          attempt += 1
          if (attempt >= maxRetries) throw ex
          logger.warn(s"Operation failed, retrying in ${delayMs}ms (attempt $attempt/$maxRetries)", ex)
          Thread.sleep(delayMs)
      }
    }
    throw new RuntimeException("Unreachable")
  }

  // Fetch observable element ids for a question (with cache + search API fallback)
  private def getObservableElements(questionId: String): List[String] = {
    if (hierarchyCache == null || questionId == null || questionId.isEmpty) return Nil
    val key = s"$OE_KEY_PREFIX$questionId"
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
      val items = Option(json.get("result")).map(_.get("items")).orElse(Option(json.get("result").get("content"))).orNull
      if (items != null && items.size() > 0) {
        val oesNode = items.get(0).get("observableElementIds")
        val oes = if (oesNode != null) oesNode.asScala.map(_.asText()).toList else Nil
        val value = s"""{"observableElementIds":${oes.map(o => "\""+o+"\"").mkString("[", ",", "]")}}"""
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

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    logger.info(s"UserEnrollmentFunction processing event: ${event.mid()}, action: ${event.action}, courseId: ${event.courseId}, activityId: ${event.activityId}")
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
