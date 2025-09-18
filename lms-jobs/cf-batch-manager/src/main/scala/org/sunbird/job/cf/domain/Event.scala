package org.sunbird.job.cf.domain

import java.util

import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "CfBatchManager"

  import scala.collection.JavaConverters._

  def action: String = readOrDefault[String]("edata.action", "")

  def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map[String, AnyRef]())

  def batchId: String = readOrDefault[String]("batchId", "")

  def userId: String = readOrDefault[String]("userId", "")

  def courseId: String = readOrDefault[String]("courseId", "")

  def status: Int = readOrDefault[Int]("edata.status", 0)

  def progress: Int = readOrDefault[Int]("edata.progress", 0)

  def enrollmentDate: String = readOrDefault[String]("edata.enrollmentDate", "")

  def enrollmentStatus: String = readOrDefault[String]("edata.enrollmentStatus", "")

  def contents: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("edata.contents", List[Map[String, AnyRef]]())

  def userData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.userData", Map[String, AnyRef]())

  def enrollmentType: String = readOrDefault[String]("edata.enrollmentType", "")

  def related: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.related", Map[String, AnyRef]())

  // Standard fields for tests (parity with dp-core Events)
  def eid(): String = readOrDefault[String]("eid", "")

  def ets(): Long = {
    val v = read[Any]("ets").orNull
    v match {
      case n: java.lang.Number => n.longValue()
      case _ => 0L
    }
  }

  def version(): String = readOrDefault[String]("ver", "")
}
