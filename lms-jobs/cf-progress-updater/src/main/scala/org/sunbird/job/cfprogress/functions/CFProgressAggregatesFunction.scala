package org.sunbird.job.cfprogress.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cfprogress.domain.Event
import org.sunbird.job.cfprogress.task.CFProgressUpdaterConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class CFProgressAggregatesFunction(config: CFProgressUpdaterConfig)
                      (implicit val stringTypeInfo: TypeInformation[String],
                       @transient var cassandraUtil: CassandraUtil = null)
    extends BaseProcessFunction[Event, String](config) {

    private[this] val logger = LoggerFactory.getLogger(classOf[CFProgressAggregatesFunction])
    private val allowedActions = List("progress-update", "cf-progress-update")

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    }

    override def close(): Unit = {
        cassandraUtil.close()
        super.close()
    }

    override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
        if (event.isValidEvent(allowedActions)) {
            logger.info("Processing CF Progress Aggregates - userId: {}, activityId: {}, activityType: {}, batchId: {}, progress: {}", 
                       event.userId, event.activityId, event.activityType, event.batchId, event.progress.asInstanceOf[Object])
            
            try {
                // TODO: Implement aggregation logic for CF progress
                logger.info("TODO: Implement aggregation logic for userId: {}, activityId: {}, batchId: {}", 
                           event.userId, event.activityId, event.batchId)
                metrics.incCounter(config.successEventCount)
            } catch {
                case e: Exception => {
                    logger.error("Failed to aggregate progress for userId: {}, activityId: {}, batchId: {}, error: {}", 
                                event.userId, event.activityId, event.batchId, e.getMessage)
                    metrics.incCounter(config.failedEventCount)
                }
            }
        } else {
            logger.warn("Invalid event received - userId: {}, activityId: {}, batchId: {}, action: {}", 
                       event.userId, event.activityId, event.batchId, event.action)
            metrics.incCounter(config.skippedEventCount)
        }
        metrics.incCounter(config.totalEventsCount)
    }

    override def metricsList(): List[String] = {
        List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbWriteCount)
    }


}
