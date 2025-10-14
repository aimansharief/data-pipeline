package org.sunbird.job.cf.domain

import org.sunbird.dp.core.domain.reader.JobRequest
import java.util
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)
  extends JobRequest(eventMap, partition, offset) with Serializable {

  // Normalize context.cdata (supports java.util.List / scala Iterable, and single Map in either java/scala)
  private val cdataList: List[Map[String, AnyRef]] = read[Any]("context.cdata") match {
    case Some(list: java.util.List[_]) =>
      list.asScala.collect { case m: util.Map[_, _] => toScalaMap(m) }.toList
    case Some(list: Iterable[_]) =>
      list.collect { case m: scala.collection.Map[_, _] @unchecked =>
        m.asInstanceOf[scala.collection.Map[Any, Any]].map { case (k, v) => k.toString -> (if (v == null) null else v.toString.asInstanceOf[AnyRef]) }.toMap
      }.toList
    case Some(m: util.Map[_, _]) => List(toScalaMap(m))
    case Some(m: scala.collection.Map[_, _]) => List(m.asInstanceOf[scala.collection.Map[Any, Any]].map { case (k, v) => k.toString -> (if (v == null) null else v.toString.asInstanceOf[AnyRef]) }.toMap)
    case _ => Nil
  }

  private def toScalaMap(m: util.Map[_, _]): Map[String, AnyRef] =
    m.asInstanceOf[util.Map[Any, Any]].asScala.map { case (k, v) => k.toString -> (if (v == null) null else v.toString.asInstanceOf[AnyRef]) }.toMap

  private def getIdByType(t: String): Option[String] = cdataList.collectFirst {
    case m if m.get("type").exists(_.toString.equalsIgnoreCase(t)) => m.get("id").map(_.toString).getOrElse("")
  }.filter(_.nonEmpty)

  // Basic fields
  def action: String = readOrDefault[String]("edata.action", "")
  def activityType: String = readOrDefault[String]("edata.activityType", "Competency Framework")

  // IDs (prefer edata, fallback to cdata, then object)
  def batchId: String = readOrDefault[String]("edata.batchId", "") match {
    case "" => getIdByType("CourseBatch").map(_.takeWhile(_ != ':')).getOrElse("")
    case b => b
  }
  def courseId: String = readOrDefault[String]("edata.courseId", "") match {
    case "" => getIdByType("Course").orElse(read[String]("object.rollup.l1")).getOrElse("")
    case c => c
  }
  def clId: String = readOrDefault[String]("edata.clId", "")
  def activityId: String = readOrDefault[String]("edata.activityId", "") match {
    case "" => getIdByType("Competency Framework")
      .orElse(getIdByType("Course"))
      .getOrElse(readOrDefault[String]("object.id", readOrDefault[String]("object.rollup.l1", "")))
    case id => id
  }

  // Users
  def userIds: List[String] = {
    val fromEdata: List[String] = read[Any]("edata.userIds") match {
      case Some(list: java.util.List[_]) =>
        list.asScala.collect { case x if x != null => x.toString.trim }.filter(_.nonEmpty).toList
      case Some(list: Iterable[_]) =>
        list.collect { case x if x != null => x.toString.trim }.filter(_.nonEmpty).toList
      case None => Nil
    }
    if (fromEdata.nonEmpty) fromEdata
    else Option(readOrDefault[String]("actor.id", "")).map(_.trim).filter(_.nonEmpty).toList
  }

  // Meta
  def status: Int = readOrDefault[Int]("edata.status", 0)
  def progress: Int = readOrDefault[Int]("edata.progress", 0)
  def enrollmentDate: String = readOrDefault[String]("edata.enrollmentDate", "")
  def enrollmentStatus: String = readOrDefault[String]("edata.enrollmentStatus", "")
  def enrollmentType: String = readOrDefault[String]("edata.enrollmentType", "")
  def name: String = readOrDefault[String]("edata.name", "")
  def description: String = readOrDefault[String]("edata.description", "")
  def startDate: String = readOrDefault[String]("edata.startDate", "")
  def endDate: String = readOrDefault[String]("edata.endDate", "")
  def enrollmentEndDate: String = readOrDefault[String]("edata.enrollmentEndDate", "")
  def createdBy: String = readOrDefault[String]("edata.createdBy", "system")
  def createdFor: List[String] = readOrDefault[List[String]]("edata.createdFor", Nil)

  // Legacy accessors
  def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map.empty)
  def contents: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("edata.contents", Nil)
  def userData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.userData", Map.empty)

  // Validation
  def validate: Option[String] = {
    if (activityId.isEmpty) Some("activityId is missing")
    else if (activityType.isEmpty) Some("activityType is missing")
    else if (!activityType.equalsIgnoreCase("Competency Framework")) Some(s"Invalid activityType: $activityType. Expected 'Competency Framework'.")
    else if (userIds.isEmpty) Some("userIds missing")
    else None
  }
}
