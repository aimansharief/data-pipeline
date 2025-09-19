package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class UserEnrollmentFunction(config: CfBatchManagerConfig)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserEnrollmentFunction])

  @transient private var cassandraUtil: CassandraUtil = _
  @transient private var hierarchyHelper: HierarchyHelper = _
  private val httpUtil = new HttpUtil
  private val lmsServiceBasePath = config.getString("service.lms.basePath", "http://lms-service:9000")
  private val clEnrollEndpoint = lmsServiceBasePath + config.getString("service.clEnroll.endpoint", "/v1/activity/enroll")
  private val courseEnrollEndpoint = lmsServiceBasePath + config.getString("service.courseEnroll.endpoint", "/course/v1/enrol")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    hierarchyHelper = new HierarchyHelper(cassandraUtil, config.dbKeyspace, config.dbTable)
    logger.info("UserEnrollmentFunction opened")
  }

  override def close(): Unit = {
    logger.info("UserEnrollmentFunction closing")
    if (cassandraUtil != null) cassandraUtil.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.failedEventCount, config.processedEventCount)
  }

  private def extractCLsAndCourses(hierarchy: java.util.Map[String, AnyRef], hierarchyHelper: HierarchyHelper): (List[String], Map[String, List[String]]) = {
    val clIds = scala.collection.mutable.ListBuffer[String]()
    val clIdToCourses = scala.collection.mutable.Map[String, List[String]]()
    val rootChildren = hierarchyHelper.getChildren(hierarchy).asScala
    rootChildren.foreach { child =>
      val pc = child.getOrDefault("primaryCategory", "").asInstanceOf[String]
      if (pc.equalsIgnoreCase("Competency Level")) {
        val levelId = child.getOrDefault("identifier", "").asInstanceOf[String]
        clIds += levelId
        val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        val courseIds = levelChildren.asScala.filter(sub => sub.getOrDefault("primaryCategory", "").asInstanceOf[String].equalsIgnoreCase("Course"))
          .map(sub => sub.getOrDefault("identifier", "").asInstanceOf[String]).filter(_.nonEmpty).toList
        clIdToCourses += (levelId -> courseIds)
      }
    }
    (clIds.toList, clIdToCourses.toMap)
  }

  private def generateBatchId(cfBatchId: String, id: String): String = s"$cfBatchId:$id"

  private def buildEnrollRequest(activityType: String, activityId: String, batchId: String, userIds: List[String]): java.util.Map[String, AnyRef] = {
    val request = new java.util.HashMap[String, AnyRef]()
    val body = new java.util.HashMap[String, AnyRef]()
    if (activityType == "Course") {
      request.put("courseId", activityId)
      request.put("batchId", batchId)
      request.put("userIds", userIds.asJava)
    } else {
      request.put("activityId", activityId)
      request.put("activityType", activityType)
      request.put("batchId", batchId)
      request.put("userIds", userIds.asJava)
    }
    body.put("request", request)
    body
  }

  private def postAndLog(httpUtil: org.sunbird.job.util.HttpUtil, endpoint: String, body: java.util.Map[String, AnyRef], logMsg: String): Unit = {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val response = httpUtil.post(endpoint, mapper.writeValueAsString(body))
    logger.info(s"$logMsg | status=${response.status}, body=${response.body}")
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    logger.info(s"User enrollment received: mid=${event.mid()} batchId=${event.batchId} activityId=${event.activityId} activityType=${event.activityType}")
    metrics.incCounter(config.totalEventCount)

    try {
      val userIds = event.userIds
      val activityId = event.activityId
      val cfBatchId = event.batchId
      val activityType = event.activityType

      val hierarchy = hierarchyHelper.getHierarchy(activityId)
      val enrollmentType = hierarchy.getOrDefault("enrollmentType", "").asInstanceOf[String]

      val (clIds, clIdToCourses) = extractCLsAndCourses(hierarchy, hierarchyHelper)

      if (enrollmentType == "Full Enrollment" && clIds.nonEmpty) {
        val clId = clIds.head
        val clBatchId = generateBatchId(cfBatchId, clId)
        val clEnrollBody = buildEnrollRequest(activityType, activityId, clBatchId, userIds)
        postAndLog(httpUtil, clEnrollEndpoint, clEnrollBody, s"Enrolled users to CL: $clId with batchId: $clBatchId")

        val courseIds = clIdToCourses.getOrElse(clId, List.empty[String])
        courseIds.foreach { courseId =>
          val courseBatchId = generateBatchId(cfBatchId, courseId)
          val courseEnrollBody = buildEnrollRequest("Course", courseId, courseBatchId, userIds)
          postAndLog(httpUtil, courseEnrollEndpoint, courseEnrollBody, s"Enrolled users to Course: $courseId with batchId: $courseBatchId")
        }
      }
      metrics.incCounter(config.processedEventCount)
    } catch {
      case e: Exception =>
        logger.error(s"User Enrollment Failed: mid=${event.mid()} batchId=${event.batchId}", e)
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
    }
  }
}
