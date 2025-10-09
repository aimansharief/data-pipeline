package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.{BaseProcessFunction, Metrics}

class CfEventRouter(config: CfBatchManagerConfig)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CfEventRouter])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    logger.info("CfEventRouter opened")
  }

  override def close(): Unit = {
    logger.info("CfEventRouter closing")
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.failedEventCount, config.processedEventCount)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    logger.info(s"Routing event: mid=${event.mid()} action=${event.action} batchId=${event.batchId}")
    metrics.incCounter(config.totalEventCount)

    try {
      event.validate match {
        case Some(error) =>
          logger.error(s"Validation failed for event: mid=${event.mid()} offset=${event.offset}. Reason: $error")
          metrics.incCounter(config.failedEventCount)
          return
        case None =>
      }
      val action = event.action
      action match {
        case config.batchUpdateAction =>
          logger.info(s"Routing to batchUpdate: batchId=${event.batchId}")
          context.output(config.batchUpdateOutputTag, event)
          metrics.incCounter(config.processedEventCount)
        case config.userEnrollmentAction =>
          logger.info(s"Routing to userEnrollment: batchId=${event.batchId} action=$action")
          context.output(config.userEnrollmentOutputTag, event)
          metrics.incCounter(config.processedEventCount)
        case config.batchCacheCreateAction =>
          logger.info(s"Routing to batchCache: batchId=${event.batchId} action=$action")
          context.output(config.batchCacheOutputTag, event)
          metrics.incCounter(config.processedEventCount)
        case _ =>
          logger.error(s"Unsupported action for mid=${event.mid()} batchId=${event.batchId} action=${action}")
          metrics.incCounter(config.failedEventCount)
      }
    } catch {
      case e: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(e.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), e)
    }
  }
}
