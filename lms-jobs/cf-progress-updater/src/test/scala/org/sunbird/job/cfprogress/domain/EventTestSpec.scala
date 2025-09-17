package org.sunbird.job.cfprogress.domain

import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.util.JSONUtil
import org.sunbird.spec.BaseTestSpec

import java.util

class EventTestSpec extends BaseTestSpec {

  "Event" should "parse valid progress update event correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_EVENT)
    val event = new Event(eventMap, 0, 10L)
    
    event.identifier should be("progress_001")
    event.action should be("progress-update")
    event.userId should be("user123")
    event.activityId should be("activity456")
    event.activityType should be("competency-framework")
    event.batchId should be("batch789")
    event.progress should be(75.5)
    event.jobName should be("CFProgressUpdater")
  }

  "Event" should "parse valid CF progress update event correctly" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_CF_PROGRESS_EVENT)
    val event = new Event(eventMap, 0, 11L)
    
    event.identifier should be("progress_002")
    event.action should be("cf-progress-update")
    event.userId should be("user456")
    event.activityId should be("activity789")
    event.activityType should be("competency-framework")
    event.batchId should be("batch123")
    event.progress should be(90.0)
  }

  "Event isValidEvent" should "return true for valid events" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_EVENT)
    val event = new Event(eventMap, 0, 10L)
    val allowedActions = List("progress-update", "cf-progress-update")
    
    event.isValidEvent(allowedActions) should be(true)
  }

  "Event isValidEvent" should "return false for events with missing userId" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_MISSING_USER_ID)
    val event = new Event(eventMap, 0, 10L)
    val allowedActions = List("progress-update", "cf-progress-update")
    
    event.isValidEvent(allowedActions) should be(false)
  }

  "Event isValidEvent" should "return false for events with invalid progress" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_INVALID_PROGRESS)
    val event = new Event(eventMap, 0, 10L)
    val allowedActions = List("progress-update", "cf-progress-update")
    
    event.isValidEvent(allowedActions) should be(false)
  }

  "Event isValidEvent" should "return false for events with unsupported action" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_UNSUPPORTED_ACTION)
    val event = new Event(eventMap, 0, 10L)
    val allowedActions = List("progress-update", "cf-progress-update")
    
    event.isValidEvent(allowedActions) should be(false)
  }

  "Event isValidEvent" should "return false for events with empty identifier" in {
    val eventMap = new util.HashMap[String, Any]()
    eventMap.put("edata", new util.HashMap[String, Any]())
    val edata = eventMap.get("edata").asInstanceOf[util.Map[String, Any]]
    edata.put("action", "progress-update")
    edata.put("userId", "user123")
    edata.put("activityId", "activity456")
    edata.put("activityType", "course")
    edata.put("batchId", "batch789")
    edata.put("progress", 75.5)
    // identifier is missing
    
    val event = new Event(eventMap, 0, 10L)
    val allowedActions = List("progress-update", "cf-progress-update")
    
    event.isValidEvent(allowedActions) should be(false)
  }

  "Event isValidEvent" should "return false for events with negative progress" in {
    val eventMap = new util.HashMap[String, Any]()
    eventMap.put("edata", new util.HashMap[String, Any]())
    val edata = eventMap.get("edata").asInstanceOf[util.Map[String, Any]]
    edata.put("action", "progress-update")
    edata.put("identifier", "progress_001")
    edata.put("userId", "user123")
    edata.put("activityId", "activity456")
    edata.put("activityType", "course")
    edata.put("batchId", "batch789")
    edata.put("progress", -10.0)
    
    val event = new Event(eventMap, 0, 10L)
    val allowedActions = List("progress-update", "cf-progress-update")
    
    event.isValidEvent(allowedActions) should be(false)
  }

  "Event isValidEvent" should "return true for events with progress at boundary values" in {
    val eventMap = new util.HashMap[String, Any]()
    eventMap.put("edata", new util.HashMap[String, Any]())
    val edata = eventMap.get("edata").asInstanceOf[util.Map[String, Any]]
    edata.put("action", "progress-update")
    edata.put("identifier", "progress_001")
    edata.put("userId", "user123")
    edata.put("activityId", "activity456")
    edata.put("activityType", "course")
    edata.put("batchId", "batch789")
    
    val allowedActions = List("progress-update", "cf-progress-update")
    
    // Test with progress = 0.0
    edata.put("progress", 0.0)
    val event1 = new Event(eventMap, 0, 10L)
    event1.isValidEvent(allowedActions) should be(true)
    
    // Test with progress = 100.0
    edata.put("progress", 100.0)
    val event2 = new Event(eventMap, 0, 11L)
    event2.isValidEvent(allowedActions) should be(true)
  }

}
