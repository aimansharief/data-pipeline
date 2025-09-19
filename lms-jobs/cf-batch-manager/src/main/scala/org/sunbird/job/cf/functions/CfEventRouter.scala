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

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    logger.info(s"Routing event: mid=${event.mid()} action=${event.action} batchId=${event.batchId}")
    metrics.incCounter(config.totalEventCount)

    try {
      val action = event.action
      // TODO: Add validation for each after it matches the action
      action match {
        case config.batchUpdateAction =>
          logger.info(s"Routing to batchUpdate: batchId=${event.batchId}")
          context.output(config.batchUpdateOutputTag, event)
        case config.userEnrollmentAction =>
          logger.info(s"Routing to userEnrollment: batchId=${event.batchId}")
          context.output(config.userEnrollmentOutputTag, event)
        case _ =>
          logger.error(s"Unsupported action for mid=${event.mid()} batchId=${event.batchId}")
          metrics.incCounter(config.failedEventCount)
      }
      metrics.incCounter(config.processedEventCount)
    } catch {
      case e: Exception =>
        logger.error(s"Error routing event mid=${event.mid()} batchId=${event.batchId}", e)
        metrics.incCounter(config.failedEventCount)
        val errorMap = Map("batchId" -> event.batchId)
        throw new InvalidEventException(e.getMessage, errorMap, e)
    }
  }
}
