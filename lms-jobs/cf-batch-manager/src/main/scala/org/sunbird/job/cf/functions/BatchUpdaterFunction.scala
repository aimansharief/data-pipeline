package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.cf.util.HierarchyHelper
import com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}
import scala.collection.JavaConverters._

class BatchUpdaterFunction(config: CfBatchManagerConfig) extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchUpdaterFunction])
  @transient private var cassandraUtil: CassandraUtil = _
  @transient private var hierarchyHelper: HierarchyHelper = _
  private val httpUtil = new HttpUtil
  private val lmsServiceBasePath = config.getString("service.lms.basePath", "http://lms-service:9000")
  private val clBatchCreateEndpoint = lmsServiceBasePath + config.getString("batch.create.endpoint.cl", "/private/v1/batch/create")
  private val courseBatchCreateEndpoint = lmsServiceBasePath + config.getString("batch.create.endpoint.course", "/private/v1/course/batch/create")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    hierarchyHelper = new HierarchyHelper(cassandraUtil, config.dbKeyspace, config.dbTable)
  }

  override def close(): Unit = {
    logger.info("BatchUpdaterFunction closing")
    if (cassandraUtil != null) cassandraUtil.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.failedEventCount, config.processedEventCount)
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
      if (activityId.nonEmpty && activityType == "CF") {
        val hierarchy = hierarchyHelper.getHierarchy(activityId)
        if (!hierarchy.isEmpty) {
          logger.info(s"Fetched hierarchy for id=$activityId")
          val courseToLevel = scala.collection.mutable.Map[String, String]()
          val levelIds = scala.collection.mutable.ListBuffer[String]()

          val rootChildren = hierarchyHelper.getChildren(hierarchy).asScala
          rootChildren.foreach { child =>
            val pc = child.getOrDefault("primaryCategory", "").asInstanceOf[String]
            if (pc.equalsIgnoreCase("Competency Level")) {
              val levelId = child.getOrDefault("identifier", "").asInstanceOf[String]
              if (levelId.nonEmpty) {
                levelIds += levelId
                val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
                levelChildren.asScala.foreach { sub =>
                  val subPc = sub.getOrDefault("primaryCategory", "").asInstanceOf[String]
                  if (subPc.equalsIgnoreCase("Course")) {
                    val courseId = sub.getOrDefault("identifier", "").asInstanceOf[String]
                    if (courseId.nonEmpty) {
                      courseToLevel += (courseId -> levelId)
                    }
                  }
                }
              }
            }
          }

          logger.info(s"Extracted CF data. levels=${levelIds.size}, courses=${courseToLevel.size}")
          logger.info(s"Course->Level: ${courseToLevel.toMap}")

          createBatchesForHierarchy(cfBatchId, levelIds.toList, courseToLevel.toMap, event)
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
                                        event: Event): Unit = {
    levelIds.foreach { levelId =>
      val batchId = generateBatchId(cfBatchId, levelId)
      val body = buildBatchRequestBody(event, levelId, "CL", batchId)
      logger.info(s"Prepared CL batch payload for levelId=$levelId")
      callBatchCreateApi(body, clBatchCreateEndpoint)
    }
    courseToLevel.foreach { case (courseId, levelId) =>
      val batchId = generateBatchId(cfBatchId, courseId)
      val body = buildBatchRequestBody(event, courseId, "Course", batchId)
      logger.info(s"Prepared Course batch payload for courseId=$courseId (levelId=$levelId)")
      callBatchCreateApi(body, courseBatchCreateEndpoint)
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
}
