package org.sunbird.job.cf.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util
import scala.collection.JavaConverters._

class UserEnrollmentFunction(config: CfBatchManagerConfig)(implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserEnrollmentFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    logger.info("UserEnrollmentFunction opened")
  }

  override def close(): Unit = {
    logger.info("UserEnrollmentFunction closing")
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.failedEventCount, config.processedEventCount)
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    logger.info(s"User enrollment received: mid=${event.mid()} batchId=${event.batchId} userId=${event.userId} activityId=${event.activityId} activityType=${event.activityType}")
    metrics.incCounter(config.totalEventCount)

    try {
      val enrollmentDate = event.enrollmentDate
      val enrollmentStatus = event.enrollmentStatus
      val userData = event.userData
      val enrollmentType = event.enrollmentType

      logger.info(s"Enrollment details: date=$enrollmentDate status=$enrollmentStatus type=$enrollmentType userData=$userData")

      // TODO: implement user enrollment logic here

      metrics.incCounter(config.processedEventCount)
    } catch {
      case e: Exception =>
        logger.error(s"User enrollment failed: mid=${event.mid()} batchId=${event.batchId} userId=${event.userId}", e)
        metrics.incCounter(config.failedEventCount)
        val errorMap = Map("batchId" -> event.batchId, "userId" -> event.userId)
        throw new InvalidEventException(e.getMessage, errorMap, e)
    }
  }
}
