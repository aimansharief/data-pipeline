package org.sunbird.job.cf

import org.sunbird.job.cf.domain.Event

import java.util
import scala.collection.JavaConverters._

object EventFixture {

  // Sample batch update event as JSON string
  val BATCH_UPDATE_EVENT: String = """{"eid":"BE_JOB_REQUEST","ets":1726656000000,"mid":"LP.12345678-1234-1234-1234-123456789012","ver":"3.0","batchId":"batch-001","userId":"user-123","activityType":"CF", "activityId":"cf-456","edata":{"action":"batch-update","batchId":"batch-001","userId":"user-123","activityType":"CF", "activityId":"cf-456","status":2,"progress":100}}"""

  // Sample user enrollment event as JSON string
  val USER_ENROLLMENT_EVENT: String = """{"eid":"BE_JOB_REQUEST","ets":1726656000001,"mid":"LP.87654321-4321-4321-4321-210987654321","ver":"3.0","batchId":"batch-002","userId":"user-456","activityType":"CF", "activityId":"cf-789","edata":{"action":"user-enrollment","batchId":"batch-002","userId":"user-456","activityType":"CF", "activityId":"cf-789","enrollmentDate":"2025-09-17","enrollmentStatus":"active"}}"""

  // Sample event with invalid action as JSON string
  val INVALID_EVENT: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1726656000002,"mid":"LP.abcdef12-3456-7890-abcd-ef1234567890","ver":"3.0","batchId":"batch-003","userId":"user-789","edata":{"action":"invalid-action","batchId":"batch-003","userId":"user-789"}}
      |""".stripMargin

  // Helper methods to get events as Maps (for backward compatibility)
  def batchUpdateEvent(): java.util.Map[String, AnyRef] = parseJsonToMap(BATCH_UPDATE_EVENT)
  def userEnrollmentEvent(): java.util.Map[String, AnyRef] = parseJsonToMap(USER_ENROLLMENT_EVENT)
  def invalidActionEvent(): java.util.Map[String, AnyRef] = parseJsonToMap(INVALID_EVENT)

  def batchUpdateEvents(): List[Event] = List(mapToEvent(batchUpdateEvent()))
  def userEnrollmentEvents(): List[Event] = List(mapToEvent(userEnrollmentEvent()))

  def allSampleEvents(): List[Event] = {
    List(mapToEvent(batchUpdateEvent()), mapToEvent(userEnrollmentEvent()), mapToEvent(invalidActionEvent()))
  }

  // Simple JSON parser (basic implementation)
  private def parseJsonToMap(jsonString: String): java.util.Map[String, AnyRef] = {
    val map = new java.util.HashMap[String, AnyRef]()

    if (jsonString.contains("batch-update")) {
      val eData = new java.util.HashMap[String, AnyRef]()
      eData.put("action", "batch-update")
      eData.put("batchId", "batch-001")
      eData.put("userId", "user-123")
      eData.put("activityId", "cf-456")
      eData.put("status", Integer.valueOf(2))
      eData.put("progress", Integer.valueOf(100))

      map.put("eid", "BE_JOB_REQUEST")
      map.put("ets", java.lang.Long.valueOf(1726656000000L))
      map.put("mid", "LP.12345678-1234-1234-1234-123456789012")
      map.put("ver", "3.0")
      map.put("batchId", "batch-001")
      map.put("userId", "user-123")
      map.put("activityId", "cf-456")
      map.put("activityType", "CF")
      map.put("edata", eData)
    } else if (jsonString.contains("user-enrollment")) {
      val eData = new java.util.HashMap[String, AnyRef]()
      eData.put("action", "user-enrollment")
      eData.put("batchId", "batch-002")
      eData.put("userId", "user-456")
      eData.put("activityId", "cf-789")
      eData.put("activityType", "CF")
      eData.put("enrollmentDate", "2025-09-17")
      eData.put("enrollmentStatus", "active")

      map.put("eid", "BE_JOB_REQUEST")
      map.put("ets", java.lang.Long.valueOf(1726656000001L))
      map.put("mid", "LP.87654321-4321-4321-4321-210987654321")
      map.put("ver", "3.0")
      map.put("batchId", "batch-002")
      map.put("userId", "user-456")
      map.put("activityId", "cf-789")
      map.put("activityType", "CF")
      map.put("edata", eData)
    } else if (jsonString.contains("invalid-action")) {
      val eData = new java.util.HashMap[String, AnyRef]()
      eData.put("action", "invalid-action")
      eData.put("batchId", "batch-003")
      eData.put("userId", "user-789")

      map.put("eid", "BE_JOB_REQUEST")
      map.put("ets", java.lang.Long.valueOf(1726656000002L))
      map.put("mid", "LP.abcdef12-3456-7890-abcd-ef1234567890")
      map.put("ver", "3.0")
      map.put("batchId", "batch-003")
      map.put("userId", "user-789")
      map.put("edata", eData)
    }

    map
  }

  // Convert map to Event object with correct type parameters
  def mapToEvent(eventMap: util.Map[String, AnyRef]): org.sunbird.job.cf.domain.Event = {
    new org.sunbird.job.cf.domain.Event(eventMap.asInstanceOf[util.Map[String, Any]], 0, 0L)
  }

}
