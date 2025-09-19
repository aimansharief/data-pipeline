package org.sunbird.job.cf.domain

import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "CfBatchManager"

  def action: String = readOrDefault[String]("edata.action", "")

  def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map[String, AnyRef]())

  def batchId: String = readOrDefault[String]("edata.batchId", "")

  def userIds: List[String] = readOrDefault[List[String]]("edata.userIds", List.empty[String])

  def activityId: String = readOrDefault[String]("edata.activityId", "")

  def activityType: String = readOrDefault[String]("edata.activityType", "")

  def status: Int = readOrDefault[Int]("edata.status", 0)

  def progress: Int = readOrDefault[Int]("edata.progress", 0)

  def enrollmentDate: String = readOrDefault[String]("edata.enrollmentDate", "")

  def enrollmentStatus: String = readOrDefault[String]("edata.enrollmentStatus", "")

  def contents: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("edata.contents", List[Map[String, AnyRef]]())

  def userData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.userData", Map[String, AnyRef]())

  def enrollmentType: String = readOrDefault[String]("edata.enrollmentType", "")

  def related: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.related", Map[String, AnyRef]())

  def name: String = readOrDefault[String]("edata.name", "")

  def description: String = readOrDefault[String]("edata.description", "")

  def startDate: String = readOrDefault[String]("edata.startDate", "")

  def endDate: String = readOrDefault[String]("edata.endDate", "")

  def enrollmentEndDate: String = readOrDefault[String]("edata.enrollmentEndDate", "")

  def createdBy: String = readOrDefault[String]("edata.createdBy", "system")

  def createdFor: List[String] = readOrDefault[List[String]]("edata.createdFor", List.empty[String])


  def isValidEvent: Boolean = {
    this.eData.nonEmpty && this.action.nonEmpty && this.activityId.nonEmpty && this.activityType.nonEmpty
  }

  def version(): String = readOrDefault[String]("ver", "")
}
