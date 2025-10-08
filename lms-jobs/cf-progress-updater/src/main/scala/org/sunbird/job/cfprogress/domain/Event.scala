package org.sunbird.job.cfprogress.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "CFProgressUpdater"

  def eid: String = readOrDefault[String]("eid", "")

  def identifier: String = readOrDefault[String]("object.rollup.l1", "")

  def action: String = readOrDefault[String]("edata.type", "")

  def userId: String = {
    val actorId = readOrDefault[String]("actor.id", "")
    if (StringUtils.isNotBlank(actorId)) actorId
    else readOrDefault[String]("object.id", "")
  }

  def activityId: String = {
    try {
      val cdata = readOrDefault[java.util.List[java.util.Map[String, Any]]]("context.cdata", new util.ArrayList())
      cdata.asScala.find(item => {
        val cdataMap = item.asInstanceOf[java.util.Map[String, Any]]
        cdataMap.get("type") == "Course"
      }).map(item => {
        val cdataMap = item.asInstanceOf[java.util.Map[String, Any]]
        cdataMap.get("id").asInstanceOf[String]
      }).getOrElse(readOrDefault[String]("object.rollup.l1", ""))
    } catch {
      case _: Exception => readOrDefault[String]("object.rollup.l1", "")
    }
  }

  def activityType: String = "Course"

  def batchId: String = {
    try {
      val cdata = readOrDefault[java.util.List[java.util.Map[String, Any]]]("context.cdata", new util.ArrayList())
      cdata.asScala.find(item => {
        val cdataMap = item.asInstanceOf[java.util.Map[String, Any]]
        cdataMap.get("type") == "CourseBatch"
      }).map(item => {
        val cdataMap = item.asInstanceOf[java.util.Map[String, Any]]
        cdataMap.get("id").asInstanceOf[String]
      }).getOrElse("")
    } catch {
      case _: Exception => ""
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
