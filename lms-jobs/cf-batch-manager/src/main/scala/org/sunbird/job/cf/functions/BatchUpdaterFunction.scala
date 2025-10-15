package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.cf.util.{CFCacheUtil, HierarchyHelper, BatchMappingUtil}
import com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.core.util.HttpUtil
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
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
  private val cacheTtl = config.redisTtlSeconds

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    hierarchyHelper = new HierarchyHelper(cassandraUtil, config.dbKeyspace, config.dbTable)
    if (redisEnabled) {
      try {
        hierarchyCache = new DataCache(config, new RedisConnect(config.cfHierarchyRedisHost, config.cfHierarchyRedisPort, config), config.cfHierarchyRedisDb, Nil)
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
        // Store the base batch mapping to CF upfront
        try BatchMappingUtil.storeBatchMapping(hierarchyCache, cfBatchId, activityId, "Competency Framework", Some(cacheTtl)) catch { case ex: Exception => logger.warn(s"BatchMappingStoreFailed batch=$cfBatchId id=$activityId type=Competency Framework", ex) }
        val hierarchy = getHierarchyWithCache(activityId)
        if (hierarchy != null && !hierarchy.isEmpty) {
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
                val picked = extractExamCourseIds(child, enrollmentType)
                picked.foreach { id =>
                  if (id != null && id.nonEmpty) examCourseIds += id
                }
                val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
                levelChildren.asScala.foreach { ch =>
                  val cpc = ch.getOrDefault("primaryCategory", "").asInstanceOf[String]
                  if (cpc.equalsIgnoreCase("Course")) {
                    val cid = ch.getOrDefault("identifier", "").asInstanceOf[String]
                    if (cid.nonEmpty) courseToLevel.put(cid, levelId)
                  }
                }
              }
            }
          }

          createBatchesForHierarchy(cfBatchId, levelIds.toList, courseToLevel.toMap, examCourseIds.toSet, event)
          // Emit an event to trigger CF batch cache build in Redis
          val eventMap = new java.util.HashMap[String, Any]()
          eventMap.putAll(event.getMap())
          val edata = new java.util.HashMap[String, Any]()
          edata.putAll(event.eData.asJava)
          edata.put("action", "cf-batch-cache-create")
          eventMap.put("edata", edata)
          val cacheEvent = new org.sunbird.job.cf.domain.Event(eventMap, event.partition, event.offset)
          context.output(config.batchCacheOutputTag, cacheEvent)
          metrics.incCounter(config.processedEventCount)
        }
      } else {
        logger.warn(s"Unsupported activity type: $activityType for event: $event")
      }
  } catch {
      case e: Exception =>
        logger.error(s"Batch Update Failed: mid=${event.mid()} batchId=${event.batchId} activityId=${event.activityId}", e)
        metrics.incCounter(config.failedEventCount)
        throw e
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
      // store mapping immediately
      try BatchMappingUtil.storeBatchMapping(hierarchyCache, batchId, levelId, "Competency Level", Some(cacheTtl)) catch { case ex: Exception => logger.warn(s"BatchMappingStoreFailed batch=$batchId id=$levelId type=Competency Level", ex) }
      callBatchCreateApi(body, clBatchCreateEndpoint)
    }
    courseToLevel.foreach { case (courseId, levelId) =>
      val batchId = generateBatchId(cfBatchId, courseId)
      val body = buildBatchRequestBody(event, courseId, "Course", batchId)
      try BatchMappingUtil.storeBatchMapping(hierarchyCache, batchId, courseId, "Course", Some(cacheTtl)) catch { case ex: Exception => logger.warn(s"BatchMappingStoreFailed batch=$batchId id=$courseId type=Course", ex) }
      callBatchCreateApi(body, courseBatchCreateEndpoint)
    }
    examCourseIds.foreach { exId =>
      if (!courseToLevel.contains(exId)) {
        val batchId = generateBatchId(cfBatchId, exId)
        val body = buildBatchRequestBody(event, exId, "Course", batchId)
        try BatchMappingUtil.storeBatchMapping(hierarchyCache, batchId, exId, "Course", Some(cacheTtl)) catch { case ex: Exception => logger.warn(s"BatchMappingStoreFailed batch=$batchId id=$exId type=Course", ex) }
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
    hierarchyHelper.getHierarchyWithCache(activityId, hierarchyCache)
  }
}
