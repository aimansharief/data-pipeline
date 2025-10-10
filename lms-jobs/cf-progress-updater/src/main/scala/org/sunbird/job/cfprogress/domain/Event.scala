package org.sunbird.job.cfprogress.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private[this] val logger = LoggerFactory.getLogger(classOf[Event])

  val jobName = "CFProgressUpdater"

  def eid: String = readOrDefault[String]("eid", "")

  def identifier: String = readOrDefault[String]("object.rollup.l1", "")

  def action: String = readOrDefault[String]("edata.type", "")

  def userId: String = {
    val actorId = readOrDefault[String]("actor.id", "")
    if (StringUtils.isNotBlank(actorId)) actorId
    else readOrDefault[String]("object.id", "")
  }

  def cdata: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("context.cdata", List())

  def activityId: String = {
    try {
      val courseId = cdata.find { item =>
        StringUtils.equalsIgnoreCase(item.getOrElse("type", "").toString, "Course")
      }.flatMap { item =>
        item.get("id").map(_.toString)
      }
      courseId.getOrElse(readOrDefault[String]("object.rollup.l1", ""))
    } catch {
      case ex: Exception =>
        logger.error(s"Error extracting activityId: ${ex.getMessage}", ex)
        readOrDefault[String]("object.rollup.l1", "")
    }
  }

  def activityType: String = "Course"

  def testBatchId: List[Map[String, AnyRef]] = cdata

  def batchId: String = {
    try {
      cdata.find { item =>
        StringUtils.equalsIgnoreCase(item.getOrElse("type", "").toString, "CourseBatch")
      }.flatMap { item =>
        item.get("id").map(_.toString)
      }.getOrElse("")
    } catch {
      case ex: Exception =>
        logger.error(s"Error extracting batchId: ${ex.getMessage}", ex)
        ""
    }
  }

  def courseId: String = activityId

  def progress: Double = readOrDefault[Double]("edata.progress", 0.0)

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def context: Map[String, AnyRef] = readOrDefault("context", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def objectData: Map[String, AnyRef] = readOrDefault("object", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def isValidEvent(allowedActions: List[String]): Boolean = {
    allowedActions.contains(action) && 
      StringUtils.isNotBlank(userId) && 
      (eid == "AUDIT")
  }

}
