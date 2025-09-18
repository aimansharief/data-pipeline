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

class BatchUpdaterFunction(config: CfBatchManagerConfig)(implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchUpdaterFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    logger.info("BatchUpdaterFunction opened")
  }

  override def close(): Unit = {
    logger.info("BatchUpdaterFunction closing")
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.failedEventCount, config.processedEventCount)
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    logger.info(s"Batch update received: mid=${event.mid()} batchId=${event.batchId} userId=${event.userId} status=${event.status} progress=${event.progress}")
    metrics.incCounter(config.totalEventCount)

    try {
      val contents = event.contents
      logger.info(s"Contents size=${contents.size} contents=${contents}")

      // TODO: implement batch update logic here

      metrics.incCounter(config.processedEventCount)
    } catch {
      case e: Exception =>
        logger.error(s"Batch update failed: mid=${event.mid()} batchId=${event.batchId} userId=${event.userId}", e)
        metrics.incCounter(config.failedEventCount)
        val errorMap = Map("batchId" -> event.batchId, "userId" -> event.userId)
        throw new InvalidEventException(e.getMessage, errorMap, e)
    }
  }
}
