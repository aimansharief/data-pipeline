package org.sunbird.job.cfprogress.domain

import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

class EventTestSpec extends BaseTestSpec {

  // ============== POSITIVE TEST CASES - EVENT COMPOSITION VALIDATION ==============

  "Event" should "parse complete enrolment event correctly with all fields" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_ENROL_COMPLETE_EVENT)
    val event = new Event(eventMap, 0, 10L)
    println(s"cdata: ${event.activityId}")
    // Validate all field extractions
    event.eid should be("AUDIT")
    event.identifier should be("do_21441672239907635218")
    event.action should be("enrol-complete")
    event.userId should be("72f74abc-ff01-4b0b-9bb3-7c4f4cca8fba")
    event.activityId should be("do_21441672239907635218")
    event.activityType should be("Course")
    event.batchId should be("0144172664199004160:do_21441672239907635218")
    event.courseId should be("do_21441672239907635218")
    event.progress should be(0.0) // default value when not specified
    event.jobName should be("CFProgressUpdater")
  }

  "Event" should "parse progress update event correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_UPDATE_EVENT)
    val event = new Event(eventMap, 0, 11L)
    
    event.eid should be("AUDIT")
    event.identifier should be("course-001")
    event.action should be("progress-update")
    event.userId should be("user-123-456-789")
    event.activityId should be("course-001")
    event.activityType should be("Course")
    event.batchId should be("batch-001")
    event.courseId should be("course-001")
    event.progress should be(75.5)
    event.jobName should be("CFProgressUpdater")
  }

  "Event" should "extract userId from actor.id when present" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_ACTOR_ID)
    val event = new Event(eventMap, 0, 12L)
    
    // When actor.id is present, it should be used
    event.userId should be("actor-user-001")
  }

  "Event" should "fallback to object.id for userId when actor.id is missing" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_OBJECT_ID_FALLBACK)
    val event = new Event(eventMap, 0, 13L)
    
    // When actor.id is missing/blank, should fallback to object.id
    event.userId should be("fallback-user-001")
  }

  "Event" should "extract activityId from cdata Course type correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_MULTIPLE_CDATA)
    val event = new Event(eventMap, 0, 14L)
    
    // Should find Course type in cdata and extract its id
    event.activityId should be("course-multi-001")
    event.courseId should be("course-multi-001")
  }

  "Event" should "extract batchId from cdata CourseBatch type correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_MULTIPLE_CDATA)
    val event = new Event(eventMap, 0, 15L)
    
    // Should find CourseBatch type in cdata and extract its id
    event.batchId should be("batch-multi-001")
  }

  "Event" should "fallback to object.rollup.l1 for activityId when cdata is missing" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_NO_CDATA)
    val event = new Event(eventMap, 0, 16L)
    
    // When cdata is missing, should fallback to object.rollup.l1
    event.activityId should be("course-from-rollup")
    event.identifier should be("course-from-rollup")
  }

  "Event" should "fallback to object.rollup.l1 for activityId when cdata is empty" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_EMPTY_CDATA)
    val event = new Event(eventMap, 0, 17L)
    
    // When cdata is empty array, should fallback to object.rollup.l1
    event.activityId should be("course-from-empty-cdata-rollup")
    event.identifier should be("course-from-empty-cdata-rollup")
  }

  "Event" should "handle zero progress correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_ZERO_PROGRESS)
    val event = new Event(eventMap, 0, 18L)
    
    event.progress should be(0.0)
    event.action should be("progress-update")
  }

  "Event" should "handle 100% progress correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_FULL_PROGRESS)
    val event = new Event(eventMap, 0, 19L)
    
    event.progress should be(100.0)
    event.action should be("progress-update")
  }

  "Event" should "handle decimal progress values correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_DECIMAL_PROGRESS)
    val event = new Event(eventMap, 0, 20L)
    
    event.progress should be(45.67)
  }

  "Event" should "always return 'Course' as activityType" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_ENROL_COMPLETE_EVENT)
    val event = new Event(eventMap, 0, 21L)
    
    // activityType is hardcoded to "Course"
    event.activityType should be("Course")
  }

  "Event" should "extract eData map correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_ENROL_COMPLETE_EVENT)
    val event = new Event(eventMap, 0, 22L)
    
    val eData = event.eData
    eData should not be empty
    eData.contains("type") should be(true)
  }

  "Event" should "extract context map correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_ENROL_COMPLETE_EVENT)
    val event = new Event(eventMap, 0, 23L)
    
    val context = event.context
    context should not be empty
    context.contains("channel") should be(true)
    context.contains("env") should be(true)
  }

  "Event" should "extract objectData map correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_ENROL_COMPLETE_EVENT)
    val event = new Event(eventMap, 0, 24L)
    
    val objectData = event.objectData
    objectData should not be empty
    objectData.contains("type") should be(true)
    objectData.contains("rollup") should be(true)
  }

  "Event isValidEvent" should "return true for valid enrol-complete event" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_ENROL_COMPLETE_EVENT)
    val event = new Event(eventMap, 0, 25L)
    val allowedActions = List("enrol-complete", "progress-update")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  "Event isValidEvent" should "return true for valid progress-update event" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_UPDATE_EVENT)
    val event = new Event(eventMap, 0, 26L)
    val allowedActions = List("enrol-complete", "progress-update")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  "Event isValidEvent" should "return true when userId is from actor.id" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_ACTOR_ID)
    val event = new Event(eventMap, 0, 27L)
    val allowedActions = List("enrol-complete")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  "Event isValidEvent" should "return true when userId is from object.id fallback" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_OBJECT_ID_FALLBACK)
    val event = new Event(eventMap, 0, 28L)
    val allowedActions = List("enrol-complete")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  "Event isValidEvent" should "return true for event with zero progress" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_ZERO_PROGRESS)
    val event = new Event(eventMap, 0, 29L)
    val allowedActions = List("progress-update")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  "Event isValidEvent" should "return true for event with full progress" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_FULL_PROGRESS)
    val event = new Event(eventMap, 0, 30L)
    val allowedActions = List("progress-update")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  "Event isValidEvent" should "return true for event with decimal progress" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_EVENT_WITH_DECIMAL_PROGRESS)
    val event = new Event(eventMap, 0, 31L)
    val allowedActions = List("progress-update")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  // ============== NEGATIVE TEST CASES ==============

  "Event isValidEvent" should "return false when userId is missing" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_MISSING_USER_ID)
    val event = new Event(eventMap, 0, 32L)
    val allowedActions = List("enrol-complete")
    
    // userId is blank, so should be invalid
    event.isValidEvent(allowedActions) should be(false)
  }

  "Event isValidEvent" should "return false when eid is not AUDIT" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_WRONG_EID)
    val event = new Event(eventMap, 0, 33L)
    val allowedActions = List("enrol-complete")
    
    // eid is BE_JOB_REQUEST instead of AUDIT
    event.isValidEvent(allowedActions) should be(false)
  }

  "Event isValidEvent" should "return false when action is not in allowed list" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_UNSUPPORTED_ACTION)
    val event = new Event(eventMap, 0, 34L)
    val allowedActions = List("enrol-complete", "progress-update")
    
    // action is "unsupported-action" which is not in allowedActions
    event.isValidEvent(allowedActions) should be(false)
  }

}
