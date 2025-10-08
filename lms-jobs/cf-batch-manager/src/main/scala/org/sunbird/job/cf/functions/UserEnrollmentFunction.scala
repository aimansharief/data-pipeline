package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.cf.util.{CFCacheUtil, EnrollmentApiUtil, HierarchyHelper}
import org.sunbird.job.cf.util.CFCacheUtil.{CLNode, CLStructure}
import org.sunbird.job.exception.InvalidEventException
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
  private val duplicateExamCheckDone = scala.collection.mutable.Set[String]()
  private val optionalCache = scala.collection.mutable.Map[String, Set[String]]()
  @transient private var userCourseStatusPs: PreparedStatement = _
  @transient private var updateOptionalPs: PreparedStatement = _
  @transient private var assessmentAggPs: PreparedStatement = _
  @transient private var assessmentQuestionPs: PreparedStatement = _

  // Cache key prefixes
  private val OE_KEY_PREFIX = "oe:"
  private val BCF_KEY_PREFIX = "bcf:"
  private val EEMAP_COURSE_PREFIX = "eemap_course:"
  private val EEMAP_CFCL_PREFIX = "eemap_cfcl:"

  // API endpoints
  private val searchUrl = config.searchBasePath + "/v3/search"

  private def batchId(base: String, id: String) = EnrollmentApiUtil.generateBatchId(base, id)
  private def enrollActivity(activityType: String, activityId: String, baseBatch: String, users: List[String], note: String): Unit = {
    if (users != null && users.nonEmpty)
      EnrollmentApiUtil.enrollActivity(httpUtil, activityType, activityId, batchId(baseBatch, activityId), users, note, clEnrollEndpoint, courseEnrollEndpoint)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    hierarchyHelper = new HierarchyHelper(cassandraUtil, config.dbKeyspace, config.dbTable)
    objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
    cfStatusMapPs = cassandraUtil.session.prepare(s"select statusmap, optional_collection from ${config.sbCollectionKeyspace}.${config.sbCollectionTable} where userid=? and activityid=? and activitytype='Competency Framework' and batchid=?")
    userCourseStatusPs = cassandraUtil.session.prepare(s"select status from ${config.userEnrollKeyspace}.${config.userEnrollTable} where userid=? and courseid=?")
    updateOptionalPs = cassandraUtil.session.prepare(s"update ${config.sbCollectionKeyspace}.${config.sbCollectionTable} set optional_collection=? where userid=? and activityid=? and activitytype='Competency Framework' and batchid=?")
    // Removed LIMIT 1 to fetch all attempts so we can consider any passing attempt
    assessmentAggPs = cassandraUtil.session.prepare(s"select total_max_score,total_score from ${config.assessmentAggKeyspace}.${config.assessmentAggTable} where course_id=? and batch_id=? and user_id=? and content_id=?")
    assessmentQuestionPs = cassandraUtil.session.prepare(s"select question from ${config.assessmentAggKeyspace}.${config.assessmentAggTable} where course_id=? and batch_id=? and user_id=? and content_id=?")
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

  private def optKey(userId: String, cfId: String, batchId: String) = s"$userId|$cfId|$batchId"

  private def fetchCfStatusAndOptional(userId: String, cfId: String, batchId: String): (Map[String, Int], Set[String]) = {
    try {
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
        try cassandraUtil.session.execute(updateOptionalPs.bind(new java.util.ArrayList[String](optional.asJava), uid, cfId, baseBatch)) catch { case ex: Exception => logger.error(s"OptionalPersistFailed user=$uid cf=$cfId optional=${optional.mkString(",")}", ex) }
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
    if (isEntranceExam) { if (clStruct.courseIds.nonEmpty) return NextCourse(clStruct.courseIds.head, clStruct.id); if (clStruct.levelExamId != null) return NextLevelExam(clStruct.levelExamId, clStruct.id) }
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
    if (strategy.isInstanceOf[EntranceExamBasedStrategy] && clStruct.entranceExamId != null && event.courseId == clStruct.entranceExamId) {
      val entranceBatchId = batchId(baseBatch, clStruct.entranceExamId)
      ensureEntranceExamMappings(resolvedCfId, clStruct.id, clStruct.entranceExamId, entranceBatchId)
      deriveEntranceBasedOptionals(resolvedCfId, clStruct, clStruct.entranceExamId, baseBatch, originalUsers)
    }
    val nextAction = computeNext(event.courseId, clStruct, orderedCLs, structures)
    val cfBatchId = baseBatch
    val users = if (enrollmentType == "Progress Based") {
      val required = if (originalUsers == null) Nil else originalUsers.filterNot(u => isOptional(u, resolvedCfId, cfBatchId, event.courseId))
      if (required.isEmpty) { logger.info(s"ProgressSkipOptionalCompletion course=${event.courseId} cf=$resolvedCfId users=${Option(originalUsers).map(_.size).getOrElse(0)}"); return }
      required
    } else originalUsers
    // Log details for optional course completion in Progress Based
    if (enrollmentType == "Progress Based") {
      val optionalCount = Option(originalUsers).getOrElse(Nil).count(u => isOptional(u, resolvedCfId, cfBatchId, event.courseId))
      if (optionalCount > 0) {
        logger.info(s"ProgressOptionalCourseCompleted course=${event.courseId} cf=$resolvedCfId optionalUsers=$optionalCount totalUsers=${Option(originalUsers).map(_.size).getOrElse(0)}")
      }
    }
    def enrollCLIfTransition(targetClId: String): Unit = if (targetClId != null && targetClId.nonEmpty && targetClId != clStruct.id && users != null && users.nonEmpty) enrollActivity("Competency Level", targetClId, baseBatch, users, s"phase=Progression nextCL=$targetClId fromCL=${clStruct.id}")
    nextAction match {
      case NextCourse(cid, clId) if users != null && users.nonEmpty =>
        val needUsers = strategy.filterUsersForCourse(cid, resolvedCfId, cfBatchId, users)
        if (needUsers.nonEmpty) {
          enrollCLIfTransition(clId)
          enrollActivity("Course", cid, baseBatch, needUsers, s"phase=Progression next=Course cl=$clId from=${event.courseId} strategy=${strategy.name}")
          // Entrance Exam Based enhancement: if this course is optional (due to entrance exam performance) immediately enroll level exam for those users
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

  private def getObservableElements(questionId: String): List[String] = {
    if (hierarchyCache != null && questionId.nonEmpty) {
      val key = s"$OE_KEY_PREFIX$questionId"
      try {
        val cached = hierarchyCache.getWithRetry(key)
        if (cached != null && cached.contains("observableElementIds")) {
          val oes = cached("observableElementIds").asInstanceOf[java.util.List[String]].asScala.toList
          return oes
        }
      } catch { case ex: Exception => }
      // Call API
      try {
        val request = s"""{"request":{"limit":1,"filters":{"identifier":"$questionId","status":["Live"]},"fields":["observableElementIds"]}}"""
        val response = httpUtil.post(searchUrl, request, Map("Content-Type" -> "application/json"))
        logger.info(s"OE SearchAPI Response $response")
        val json = objectMapper.readTree(response.body)
        val items = json.get("result").get("items")
        if (items.size() > 0) {
          val oes = items.get(0).get("observableElementIds").asScala.map(_.asText()).toList
          val value = s"""{"observableElementIds":${oes.map("\"" + _ + "\"").mkString("[", ",", "]")}}"""
          hierarchyCache.setWithRetry(key, value)
          oes
        } else Nil
      } catch { case ex: Exception => logger.warn(s"OEApiFailed questionId=$questionId", ex); Nil }
    } else Nil
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
    recurse(root)
    result.toList
  }

  private def getQuestionIdsForContent(courseId: String, batchId: String, contentId: String): List[String] = {
    try {
      val rs = withRetry(cassandraUtil.session.execute(assessmentQuestionPs.bind(courseId, batchId, contentId)))
      val row = rs.one()
      if (row != null && !row.isNull("question")) {
        val questions = row.getList("question", classOf[java.util.Map[_, _]]).asScala
        questions.map(q => Option(q.get("id")).map(_.toString).getOrElse("")).filter(_.nonEmpty).toList
      } else Nil
    } catch { case ex: Exception => logger.warn(s"QuestionIdsFetchFailed course=$courseId batch=$batchId content=$contentId", ex); Nil }
  }

  private def contentScorePercent(courseId: String, batchId: String, userId: String, contentId: String): Map[String, Double] = {
    try {
      val rs = withRetry(cassandraUtil.session.execute(assessmentQuestionPs.bind(courseId, batchId, userId, contentId)))
      val row = rs.one()
      if (row != null && !row.isNull("question")) {
        val questions = row.getList("question", classOf[java.util.Map[_, _]]).asScala
        questions.map { q =>
          val qMap = q.asInstanceOf[java.util.Map[String, AnyRef]]
          val qId = Option(qMap.get("id")).map(_.toString).getOrElse("")
          val score = Option(qMap.get("score")).map(_.toString.toDouble).getOrElse(0.0)
          val maxScore = Option(qMap.get("max_score")).map(_.toString.toDouble).getOrElse(0.0)
          val pct = if (maxScore > 0) (score / maxScore) * 100 else 0.0
          qId -> pct
        }.filter(_._1.nonEmpty).toMap
      } else Map.empty[String, Double]
    } catch { case ex: Exception => logger.warn(s"ContentScorePercentFailed course=$courseId batch=$batchId user=$userId content=$contentId", ex); Map.empty[String, Double] }
  }

  private def deriveEntranceBasedOptionals(cfId: String, clStruct: CLStructure, entranceExamId: String, baseBatch: String, users: List[String]): Unit = {
    if (users == null || users.isEmpty || hierarchyCache == null) return
    val cfBatchId = baseBatch
    val entranceBatchId = batchId(baseBatch, entranceExamId)
    val (courseMap, courseQToContent, clMap, clQToContent) = loadEntranceMappingsFromCache(entranceExamId, cfId, clStruct.id)
    if (courseMap.isEmpty || clMap.isEmpty) return

    // Build contentId -> courseId map for this CL
    val contentToCourse = buildContentToCourseMap(cfId, clStruct)
    logger.info(s"EntranceOptionalDerivationStarted cf=$cfId cl=${clStruct.id} entranceExam=$entranceExamId users=${users.size} contentToCourseSize=${contentToCourse.size}")

    // Build observable -> courses mapping from cached CL mappings
    val observableToCourses = scala.collection.mutable.Map[String, Set[String]]()
    clMap.foreach { case (qId, obsList) =>
      val contentId = clQToContent.get(qId)
      val courseId = contentId.flatMap(contentToCourse.get)
      if (courseId.isDefined) {
        obsList.foreach { obs =>
          val existing = observableToCourses.getOrElse(obs, Set.empty)
          observableToCourses.put(obs, existing + courseId.get)
        }
      }
    }
    logger.info(s"ObservableToCoursesBuilt cf=$cfId cl=${clStruct.id} observableToCoursesSize=${observableToCourses.size}")

    users.foreach { uid =>
      logger.info(s"ProcessingUserForOptionals cf=$cfId cl=${clStruct.id} user=$uid")
      val passedObservables = courseMap.keys.flatMap { qId =>
        val contentId = courseQToContent.get(qId)
        if (contentId.isDefined) {
          val scores = contentScorePercent(entranceExamId, entranceBatchId, uid, contentId.get)
          scores.get(qId) match {
            case Some(pct) if pct >= config.entranceExamOptionalThreshold => courseMap.getOrElse(qId, Nil)
            case _ => Nil
          }
        } else Nil
      }.toSet
      logger.info(s"PassedObservablesForUser cf=$cfId cl=${clStruct.id} user=$uid passedObservables=${passedObservables.mkString(",")}")

      if (passedObservables.nonEmpty) {
        val newlyOptionalCourses = passedObservables.flatMap(obs => observableToCourses.get(obs)).flatten.toSet.intersect(clStruct.courseIds.toSet)
        logger.info(s"NewlyOptionalCoursesForUser cf=$cfId cl=${clStruct.id} user=$uid newlyOptionalCourses=${newlyOptionalCourses.mkString(",")}")
        if (newlyOptionalCourses.nonEmpty) {
          val key = optKey(uid, cfId, cfBatchId)
          val current = optionalCache.getOrElse(key, Set.empty).union(newlyOptionalCourses)
          logger.info(s"UpdatingOptionalCollection cf=$cfId cl=${clStruct.id} user=$uid currentOptionalCourses=${current.mkString(",")}")
          if (current.nonEmpty) {
            try cassandraUtil.session.execute(updateOptionalPs.bind(new java.util.ArrayList[String](current.asJava), uid, cfId, cfBatchId)) catch { case ex: Exception => logger.warn(s"EntranceBasedOptionalPersistFailed user=$uid cf=$cfId cl=${clStruct.id}", ex) }
            optionalCache.put(key, current)
            logger.info(s"EntranceBasedOptionals user=$uid cf=$cfId cl=${clStruct.id} threshold=${config.entranceExamOptionalThreshold} courses=${current.mkString(",")}")
          }
        }
      }
    }
  }

  // Traverse CL subtree to assign each leaf content to its parent course (whose identifier matches one of clStruct.courseIds)
  private def buildContentToCourseMap(cfId: String, clStruct: CLStructure): Map[String, String] = {
    val courseIds = clStruct.courseIds.toSet
    if (courseIds.isEmpty) return Map.empty
    try {
      val cfHierarchy = hierarchyHelper.getHierarchy(cfId)
      if (cfHierarchy == null) return Map.empty
      val clNodeOpt = Option(cfHierarchy.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]).getOrElse(java.util.Collections.emptyList()).asScala.find { c => Option(c.get("identifier")).exists(_.toString == clStruct.id) }
      if (clNodeOpt.isEmpty) return Map.empty
      val mapping = scala.collection.mutable.Map[String, String]()
      def recurse(node: java.util.Map[String, AnyRef], currentCourse: Option[String]): Unit = {
        val id = Option(node.get("identifier")).map(_.toString).getOrElse("")
        val nextCourse = if (id.nonEmpty && courseIds.contains(id)) Some(id) else currentCourse
        val children = Option(node.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]).getOrElse(java.util.Collections.emptyList())
        if (children.isEmpty && nextCourse.isDefined && id.nonEmpty && !courseIds.contains(id)) {
          // leaf content inside a course
          mapping.put(id, nextCourse.get)
        } else {
          children.asScala.foreach(ch => recurse(ch, nextCourse))
        }
      }
      recurse(clNodeOpt.get, None)
      mapping.toMap
    } catch { case ex: Exception => logger.warn(s"ContentToCourseBuildFailed cf=$cfId cl=${clStruct.id}", ex); Map.empty }
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

  private def extractObservableElementMapping(courseId: String, batchId: String, root: java.util.Map[String, AnyRef]): (Map[String, List[String]], Map[String, String]) = {
    val contentIds = extractContentIds(root)
    val result = scala.collection.mutable.Map[String, List[String]]()
    val qToContent = scala.collection.mutable.Map[String, String]()
    contentIds.foreach { contentId =>
      val qIds = getQuestionIdsForContent(courseId, batchId, contentId)
      qIds.foreach { qId =>
        val obs = getObservableElements(qId)
        if (obs.nonEmpty) {
          result.put(qId, obs)
          qToContent.put(qId, contentId)
        }
      }
    }
    (result.toMap, qToContent.toMap)
  }

  private def ensureEntranceExamMappings(cfId: String, clId: String, entranceExamId: String, batchId: String): Unit = {
    if (hierarchyCache == null || cfId == null || cfId.isEmpty || clId == null || clId.isEmpty || entranceExamId == null || entranceExamId.isEmpty || batchId == null || batchId.isEmpty) return
    val courseKey = s"$EEMAP_COURSE_PREFIX$entranceExamId"
    val clKey = s"$EEMAP_CFCL_PREFIX$cfId:$clId"
    def cacheMissing(k: String): Boolean = {
      try { val m = hierarchyCache.getWithRetry(k); m == null || m.isEmpty } catch { case _: Exception => true }
    }
    val buildCourse = cacheMissing(courseKey)
    val buildCl = cacheMissing(clKey)
    if (!buildCourse && !buildCl) return
    def toJsonArray(list: List[String]) = list.map(v => "\"" + v + "\"").mkString("[", ",", "]")
    def mappingJson(map: Map[String, List[String]]) = map.map { case (k, v) => "\"" + k + "\":" + toJsonArray(v) }.mkString(",")
    def qToContentJson(map: Map[String, String]) = map.map { case (k, v) => "\"" + k + "\":\"" + v + "\"" }.mkString(",")
    try {
      if (buildCourse) {
        val courseHierarchy = hierarchyHelper.getHierarchy(entranceExamId)
        val (map, qToContent) = extractObservableElementMapping(entranceExamId, batchId, courseHierarchy)
        val json = mappingJson(map)
        val qJson = qToContentJson(qToContent)
        val value = "{\"courseId\":\"" + entranceExamId + "\",\"mapping\":{" + json + "},\"qToContent\":{" + qJson + "}}"
        hierarchyCache.setWithRetry(courseKey, value)
        logger.info(s"EntranceExamCourseObservableMapStored course=$entranceExamId size=${map.size}")
      }
    } catch { case ex: Exception => logger.warn(s"EntranceExamCourseObservableMapFailed course=$entranceExamId", ex) }
    try {
      if (buildCl) {
        val cfHierarchy = hierarchyHelper.getHierarchy(cfId)
        val clNodeOpt = Option(cfHierarchy.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]).getOrElse(java.util.Collections.emptyList()).asScala.find { c => Option(c.get("identifier")).exists(_.toString == clId) }
        clNodeOpt.foreach { clNode =>
            val (map, qToContent) = extractObservableElementMapping(entranceExamId, batchId, clNode)
            val json = mappingJson(map)
            val qJson = qToContentJson(qToContent)
            val value = "{\"cfId\":\"" + cfId + "\",\"clId\":\"" + clId + "\",\"mapping\":{" + json + "},\"qToContent\":{" + qJson + "}}"
            hierarchyCache.setWithRetry(clKey, value)
            logger.info(s"EntranceExamCFCLObservableMapStored cf=$cfId cl=$clId size=${map.size}")
        }
      }
    } catch { case ex: Exception => logger.warn(s"EntranceExamCFCLObservableMapFailed cf=$cfId cl=$clId", ex) }
  }

  private def loadEntranceMappingsFromCache(entranceExamCourseId: String, cfId: String, clId: String): (Map[String, List[String]], Map[String, String], Map[String, List[String]], Map[String, String]) = {
    if (hierarchyCache == null) return (Map.empty, Map.empty, Map.empty, Map.empty)
    def read(key: String): (Map[String, List[String]], Map[String, String]) = {
      try {
        val m = hierarchyCache.getWithRetry(key)
        if (m == null || m.isEmpty) return (Map.empty, Map.empty)
        val mapping = m.get("mapping") match {
          case Some(jm: java.util.Map[_, _]) =>
            jm.asScala.collect {
              case (k: String, v: java.util.List[_]) => k -> v.asScala.toList.map(_.toString).filter(_.nonEmpty)
            }.toMap[String, List[String]]
          case Some(sm: scala.collection.Map[_, _]) =>
            sm.collect {
              case (k: String, v: java.util.List[_]) => k -> v.asScala.toList.map(_.toString).filter(_.nonEmpty)
              case (k: String, v: List[_]) => k -> v.map(_.toString).filter(_.nonEmpty)
            }.toMap[String, List[String]]
          case _ => Map.empty[String, List[String]]
        }
        val qToContent = m.get("qToContent") match {
          case Some(jm: java.util.Map[_, _]) =>
            jm.asScala.collect {
              case (k: String, v: String) => k -> v
            }.toMap[String, String]
          case _ => Map.empty[String, String]
        }
        (mapping, qToContent)
      } catch { case _: Exception => (Map.empty, Map.empty) }
    }
    val courseKey = s"$EEMAP_COURSE_PREFIX$entranceExamCourseId"
    val clKey = s"$EEMAP_CFCL_PREFIX$cfId:$clId"
    val (courseMap, courseQToContent) = read(courseKey)
    val (clMap, clQToContent) = read(clKey)
    logger.info(s"EntranceMappingsLoaded entranceExam=$entranceExamCourseId cf=$cfId cl=$clId courseMapSize=${courseMap.size} clMapSize=${clMap.size}")
    (courseMap, courseQToContent, clMap, clQToContent)
  }

  private def warnDuplicateExamIds(cfId: String, structures: Map[String, CLStructure]): Unit = {
    val exams = structures.values.flatMap(s => List(Option(s.entranceExamId), Option(s.levelExamId)).flatten).toList
    val duplicates = exams.groupBy(identity).collect { case (id, list) if list.size > 1 => id }
    if (duplicates.nonEmpty) {
      logger.warn(s"DuplicateExamIds cf=$cfId duplicates=${duplicates.mkString(",")}")
    }
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    try {
      progression(event)
      metrics.incCounter(config.processedEventCount)
    } catch {
      case ex: Exception =>
        logger.error(s"UserEnrollmentFunction processing failed for event: ${event.mid()}", ex)
        metrics.incCounter(config.failedEventCount)
    }
  }
}
