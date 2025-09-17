package org.sunbird.job.cfprogress.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "CFProgressUpdater"

  def identifier: String = readOrDefault[String]("edata.identifier", "")

  def action: String = readOrDefault[String]("edata.action", "")

  def userId: String = readOrDefault[String]("edata.userId", "")

  def activityId: String = readOrDefault[String]("edata.activityId", "")

  def activityType: String = readOrDefault[String]("edata.activityType", "")

  def batchId: String = readOrDefault[String]("edata.batchId", "")

  def progress: Double = readOrDefault[Double]("edata.progress", 0.0)

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def isValidEvent(allowedActions: List[String]): Boolean = {
    // Dummy implementation - validate that required fields are present and action is allowed
    allowedActions.contains(action) && 
      StringUtils.isNotBlank(identifier) && 
      StringUtils.isNotBlank(userId) && 
      StringUtils.isNotBlank(activityId) &&
      StringUtils.isNotBlank(activityType) &&
      progress >= 0.0 && progress <= 100.0
  }

}
