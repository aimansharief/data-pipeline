package org.sunbird.job.cf.util

import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.HttpUtil

import scala.collection.JavaConverters._

object EnrollmentApiUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  // Updated: if cfBatchId already composite (contains ':') we treat portion before first ':' as base
  def generateBatchId(cfBatchId: String, id: String): String = {
    if (cfBatchId == null || cfBatchId.isEmpty) return id
    val base = {
      val idx = cfBatchId.indexOf(':')
      if (idx >= 0) cfBatchId.substring(0, idx) else cfBatchId
    }
    s"$base:$id"
  }

  def buildEnrollRequest(activityType: String, activityId: String, batchId: String, userIds: List[String]): String = {
    val requestData = new java.util.HashMap[String, AnyRef]()
    if (activityType == "Course") requestData.put("courseId", activityId) else {
      requestData.put("activityId", activityId)
      requestData.put("activityType", activityType)
    }
    requestData.put("batchId", batchId)
    requestData.put("userIds", userIds.asJava)
    val wrapper = new java.util.HashMap[String, AnyRef]()
    wrapper.put("request", requestData)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    mapper.writeValueAsString(wrapper)
  }

  def enrollActivity(httpUtil: HttpUtil, activityType: String, id: String, batchId: String, users: List[String], 
                    logCtx: String, clEnrollEndpoint: String, courseEnrollEndpoint: String): Unit = {
    val body = buildEnrollRequest(activityType, id, batchId, users)
    val ep = if (activityType == "Course") courseEnrollEndpoint else clEnrollEndpoint
    logger.info(s"EnrollRequest $logCtx activityType=$activityType id=$id batchId=$batchId url=$ep users=${users.mkString(",")} payload=$body")
    val response = try { httpUtil.post(ep, body) } catch { case ex: Exception =>
      logger.error(s"EnrollHTTPError $logCtx activityType=$activityType id=$id batchId=$batchId url=$ep error=${ex.getMessage}", ex); throw ex }
    logger.info(s"EnrollResponse $logCtx activityType=$activityType id=$id batchId=$batchId status=${response.status} body=${response.body}")
  }
}
