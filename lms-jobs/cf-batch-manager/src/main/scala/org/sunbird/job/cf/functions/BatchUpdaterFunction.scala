package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.cf.util.{CFCacheUtil, HierarchyHelper}
import com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.cache.{DataCache, RedisConnect}
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonFactory
import scala.collection.JavaConverters._

class BatchUpdaterFunction(config: CfBatchManagerConfig) extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchUpdaterFunction])
  @transient private var cassandraUtil: CassandraUtil = _
  @transient private var hierarchyHelper: HierarchyHelper = _
  @transient private var hierarchyCache: DataCache = _
  private val httpUtil = new HttpUtil

  private val clBatchCreateEndpoint = config.lmsBasePath + config.clBatchCreateRoute
  private val courseBatchCreateEndpoint = config.lmsBasePath + config.courseBatchCreateRoute
  private val redisEnabled = true

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    hierarchyHelper = new HierarchyHelper(cassandraUtil, config.dbKeyspace, config.dbTable)
    if (redisEnabled) {
      try {
        hierarchyCache = new DataCache(config, new RedisConnect(config), config.cfHierarchyRedisDb, Nil)
        hierarchyCache.init()
        logger.info(s"Hierarchy DataCache initialised db=${config.cfHierarchyRedisDb}")
      } catch { case ex: Exception => logger.warn("Hierarchy DataCache init failed; proceeding without cache", ex) }
    }
  }

  override def close(): Unit = {
    logger.info("BatchUpdaterFunction closing")
    try { if (hierarchyCache != null) hierarchyCache.close() } catch { case ex: Exception => logger.warn("Hierarchy cache close failed", ex) }
    try { if (cassandraUtil != null) cassandraUtil.close() } catch { case ex: Exception => logger.warn("Cassandra close failed", ex) }
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.failedEventCount, config.processedEventCount)
  }

  private def extractExamCourseIds(child: java.util.Map[String, AnyRef], enrollmentType: String): List[String] = {
    val levelId = child.getOrDefault("identifier", "").asInstanceOf[String]

    val levelExam = if (child.containsKey("levelExam")) child.get("levelExam").asInstanceOf[java.util.Map[String, AnyRef]] else null
    val levelExamId = if (levelExam != null) levelExam.getOrDefault("collectionId", "").asInstanceOf[String] else ""

    val entranceExam = if (child.containsKey("entranceExam")) child.get("entranceExam").asInstanceOf[java.util.Map[String, AnyRef]] else null
    val entranceEnabledRaw = if (entranceExam != null) entranceExam.getOrDefault("enabled", "").asInstanceOf[String] else ""
    val entranceEnabled = enrollmentType.equalsIgnoreCase("Entrance Exam Based") && entranceEnabledRaw.equalsIgnoreCase("Yes")
    val entranceExamId = if (entranceEnabled && entranceExam != null) entranceExam.getOrDefault("collectionId", "").asInstanceOf[String] else ""

    val picked = scala.collection.mutable.ListBuffer[String]()
    if (levelExamId.nonEmpty) picked += levelExamId
    if (entranceExamId.nonEmpty) picked += entranceExamId

    logger.info(s"ExamExtraction: levelId=$levelId enrollmentType=$enrollmentType levelExamId=${if (levelExamId.isEmpty) "-" else levelExamId} entranceExamEnabled=$entranceEnabled entranceExamId=${if (entranceExamId.isEmpty) "-" else entranceExamId} selectedIds=${picked.mkString(",")}")

    picked.toList
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    val activityId = event.activityId
    val activityType = event.activityType
    val cfBatchId = event.batchId
    logger.info(s"BatchUpdaterFunction :: Batch update received event: $event activityId = $activityId  activityType = $activityType")
    metrics.incCounter(config.totalEventCount)
    try {
      if (activityType.equalsIgnoreCase("Competency Framework")) {
        val hierarchy = getHierarchyWithCache(activityId)
        if (hierarchy != null && !hierarchy.isEmpty) {
          val hierarchyJson = new ObjectMapper().writeValueAsString(hierarchy)
          logger.info(s"Hierarchy loaded for $activityId")

          val enrollmentType = hierarchy.getOrDefault("enrollmentType", "").asInstanceOf[String]
          logger.info(s"Fetched hierarchy for id=$activityId enrollmentType=$enrollmentType")
          val courseToLevel = scala.collection.mutable.Map[String, String]()
          val levelIds = scala.collection.mutable.ListBuffer[String]()
          val examCourseIds = scala.collection.mutable.Set[String]()

          val rootChildren = hierarchyHelper.getChildren(hierarchy).asScala
          rootChildren.foreach { child =>
            val pc = child.getOrDefault("primaryCategory", "").asInstanceOf[String]
            if (pc.equalsIgnoreCase("Competency Level")) {
              val levelId = child.getOrDefault("identifier", "").asInstanceOf[String]
              if (levelId.nonEmpty) {
                levelIds += levelId
                // Normal child courses
                val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
                levelChildren.asScala.foreach { sub =>
                  val subPc = sub.getOrDefault("primaryCategory", "").asInstanceOf[String]
                  if (subPc.equalsIgnoreCase("Course")) {
                    val courseId = sub.getOrDefault("identifier", "").asInstanceOf[String]
                    if (courseId.nonEmpty) courseToLevel += (courseId -> levelId)
                  }
                }
                // Exam related courses (levelExam / entranceExam)
                val examIds = extractExamCourseIds(child, enrollmentType)
                examIds.foreach(exId => examCourseIds += exId)
              }
            }
          }

          logger.info(s"Extracted CF data. levels=${levelIds.size}, courses=${courseToLevel.size}, examCourses=${examCourseIds.size}")
          logger.info(s"Course->Level: ${courseToLevel.toMap} examCourses=${examCourseIds.mkString(",")}")

          createBatchesForHierarchy(cfBatchId, levelIds.toList, courseToLevel.toMap, examCourseIds.toSet, event)
          // Emit an event to trigger CF batch cache build in Redis
          val cacheEvent = event.setAction("cf-batch-cache-create")
          context.output(config.batchCacheOutputTag, cacheEvent)
          metrics.incCounter(config.processedEventCount)
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Batch Update Failed: mid=${event.mid()} batchId=${event.batchId} activityId=${event.activityId}", e)
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
    }
  }

  private def createBatchesForHierarchy(cfBatchId: String,
                                        levelIds: List[String],
                                        courseToLevel: Map[String, String],
                                        examCourseIds: Set[String],
                                        event: Event): Unit = {
    levelIds.foreach { levelId =>
      val batchId = generateBatchId(cfBatchId, levelId)
      val body = buildBatchRequestBody(event, levelId, "Competency Level", batchId)
      logger.info(s"Prepared Competency Level batch payload: $body for levelId=$levelId")
      callBatchCreateApi(body, clBatchCreateEndpoint)
    }
    // Standard courses
    courseToLevel.foreach { case (courseId, levelId) =>
      val batchId = generateBatchId(cfBatchId, courseId)
      val body = buildBatchRequestBody(event, courseId, "Course", batchId)
      logger.info(s"Prepared Course batch payload: $body for courseId=$courseId (levelId=$levelId)")
      callBatchCreateApi(body, courseBatchCreateEndpoint)
    }
    // Exam courses (avoid duplicates already in courseToLevel)
    examCourseIds.foreach { exId =>
      if (!courseToLevel.contains(exId)) {
        val batchId = generateBatchId(cfBatchId, exId)
        val body = buildBatchRequestBody(event, exId, "Course", batchId)
        logger.info(s"Prepared Exam Course batch payload: $body for courseId=$exId")
        callBatchCreateApi(body, courseBatchCreateEndpoint)
      }
    }
  }

  private def buildBatchRequestBody(event: Event, activityId: String, activityType: String, batchId: String): java.util.Map[String, AnyRef] = {
    val request = new java.util.HashMap[String, AnyRef]()
    val body = new java.util.HashMap[String, AnyRef]()
    if (activityType == "Course") {
      request.put("courseId", activityId)
    } else {
      request.put("activityId", activityId)
      request.put("activityType", activityType)
    }
    request.put("batchId", batchId)
    request.put("name", event.name)
    request.put("description", event.description)
    request.put("startDate", event.startDate)
    request.put("endDate", event.endDate)
    request.put("createdBy", event.createdBy)
    request.put("enrollmentType", event.enrollmentType)
    request.put("enrollmentEndDate", event.enrollmentEndDate)
    request.put("createdFor", event.createdFor.asJava)
    body.put("request", request)
    body
  }

  private def callBatchCreateApi(body: java.util.Map[String, AnyRef], endpoint: String): Unit = {
    val mapper = new ObjectMapper()
    val requestBody = mapper.writeValueAsString(body)
    val requestMap = body.get("request").asInstanceOf[java.util.Map[String, AnyRef]]
    val idForLog =
      if (requestMap.containsKey("courseId")) s"courseId=${requestMap.get("courseId")}" 
      else if (requestMap.containsKey("activityId")) s"activityId=${requestMap.get("activityId")}" 
      else "unknownId"
    val response = httpUtil.post(endpoint, requestBody)
    if (response.status == 200 ) {
      logger.info(s"Batch creation successful for $idForLog at $endpoint: ${response.body}")
    } else {
      logger.error(s"Batch creation failed for $idForLog at $endpoint: status=${response.status}, body=${response.body}")
    }
  }

  private def generateBatchId(cfBatchId: String, id: String): String = s"$cfBatchId:$id"

  private def getHierarchyWithCache(activityId: String): java.util.Map[String, AnyRef] = {
    if (hierarchyCache != null) {
      val key = s"hierarchy:$activityId"
      try {
        val cachedJson = hierarchyCache.getWithRetry(key)
        if (cachedJson != null && !cachedJson.isEmpty) {
          val mapper = new ObjectMapper()
          val json = cachedJson.get("hierarchy").map(_.asInstanceOf[String]).getOrElse("")
          if (json.nonEmpty) return mapper.readValue(json, classOf[java.util.Map[String, AnyRef]])
        }
      } catch { case ex: Exception => logger.warn(s"Cache read failed for $activityId", ex) }
    }
    val hierarchy = hierarchyHelper.getHierarchy(activityId)
    if (hierarchy != null && hierarchyCache != null) {
      try {
        val mapper = new ObjectMapper()
        val json = mapper.writeValueAsString(hierarchy)
        val value = s"""{"hierarchy":"${json.replace("\"", "\\\"")}"}"""
        hierarchyCache.setWithRetry(s"hierarchy:$activityId", value)
      } catch { case ex: Exception => logger.warn(s"Cache write failed for $activityId", ex) }
    }
    hierarchy
  }
}
