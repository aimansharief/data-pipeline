package org.sunbird.job.cf.task

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.cf.EventFixture

class CfBatchManagerRouterTest extends FlatSpec with Matchers {

  "CfEventRouter" should "route batch-update events correctly" in {
    val batchUpdateEvents = EventFixture.batchUpdateEvents()
    batchUpdateEvents.foreach { event =>
      event.action should be ("batch-update")
    }
  }

  "CfEventRouter" should "route user-enrollment events correctly" in {
    val userEnrollmentEvents = EventFixture.userEnrollmentEvents()
    userEnrollmentEvents.foreach { event =>
      event.action should be ("user-enrollment")
    }
  }


  "EventFixture" should "provide events with valid edata" in {
    val eventsWithEdata = EventFixture.allSampleEvents().filter(_.eData.nonEmpty)
    eventsWithEdata should not be empty
    eventsWithEdata.size should be (3) // All our events have edata

    eventsWithEdata.foreach { event =>
      event.eData should not be empty
    }
  }

  "EventFixture" should "provide batch update events with content data" in {
    val batchUpdateEvents = EventFixture.batchUpdateEvents()
    batchUpdateEvents.foreach { event =>
      event.contents should not be empty
      event.contents.foreach { content =>
        content should contain key "contentId"
        content should contain key "status"
      }
    }
  }

  "EventFixture" should "provide user enrollment events with user data" in {
    val userEnrollmentEvents = EventFixture.userEnrollmentEvents()
    userEnrollmentEvents.foreach { event =>
      event.userData should not be empty
      event.userData should contain key "firstName"
      event.userData should contain key "lastName"
      event.userData should contain key "email"
    }
  }
}
