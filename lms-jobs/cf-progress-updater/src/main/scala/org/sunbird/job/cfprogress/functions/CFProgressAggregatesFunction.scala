package org.sunbird.job.cfprogress.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
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
    private val allowedActions = List("enrol-complete")
    private var cache: DataCache = _

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
        cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
        cache.init()
    }

    override def close(): Unit = {
        cassandraUtil.close()
        cache.close()
        super.close()
    }

    override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
        if (event.isValidEvent(allowedActions)) {
            logger.info("Processing CF Progress Aggregates - userId: {}, activityId: {}, activityType: {}, batchId: {}, progress: {}", 
                       event.userId, event.activityId, event.activityType, event.batchId, event.progress.asInstanceOf[Object])
            
            try {
                // Fetch user enrolments where status is 2 and extract fields
                val userEnrolments: List[Map[String, AnyRef]] = fetchUserEnrolments(event.userId, metrics)
                logger.info("Fetched and extracted {} user enrolment records for userId: {} with status = 2", 
                           userEnrolments.size, event.userId)
                
                // Extract batchIds completed by the user
                val completedBatchIds: Set[String] = userEnrolments.map(enrolment => 
                    enrolment.getOrElse("batchid", "").toString
                ).filter(_.nonEmpty).toSet
                
                logger.info("User {} has completed {} batches: {}", 
                           event.userId, completedBatchIds.size.asInstanceOf[Object], completedBatchIds.mkString(", "))
                
                val ancestors = readFromCache(key = s"${event.batchId}-${config.ancestors}", metrics)
                if (ancestors.nonEmpty) {
                    ancestors.foreach(parentId => {
                        val leafNodeIds = readFromCache(key = s"${parentId}-${config.leafNodes}", metrics)
                        if (leafNodeIds == null || leafNodeIds.isEmpty) {
                            throw new RuntimeException(s"Cache missing or empty for key: ${parentId}-${config.leafNodes}. ParentId ${parentId} must have leafNodeIds.")
                        }
                        
                        // Compute intersection of leafNodeIds and completed batchIds
                        val leafNodeIdSet = leafNodeIds.toSet
                        val completedBatchIdsForParent = leafNodeIdSet.intersect(completedBatchIds).toList
                        val completedCount = completedBatchIdsForParent.size
                        val totalCount = leafNodeIdSet.size
                        val progressPercentage = if (totalCount > 0) (completedCount * 100.0 / totalCount) else 0.0
                        
                        logger.info("ParentId: {} | Total leafNodes: {} | Completed: {} | Progress: {}% | Completed batchIds: {}", 
                                   parentId, totalCount.asInstanceOf[Object], completedCount.asInstanceOf[Object], 
                                   progressPercentage.asInstanceOf[Object], completedBatchIdsForParent.mkString(", "))
                    })
                }
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

    /**
     * Fetch user enrolments from Cassandra DB where status is 2
     * Exceptions (e.g., DB connection issues) will propagate to caller
     *
     * @param userId  The user ID to fetch enrolments for
     * @param metrics Metrics object to track DB operations
     * @return List of Map[String, AnyRef] objects containing user enrolment records (empty list if none found)
     */
    private def fetchUserEnrolments(userId: String, metrics: Metrics): List[Map[String, AnyRef]] = {
        val columns = Map("userid" -> userId, "status" -> Integer.valueOf(2))
        Option(readFromDB(columns, config.courseKeyspace, config.courseEnrolmentsTable, metrics))
            .getOrElse(List.empty[Row]).map(row => {
                    Map(
                        "batchid" -> Option(row.getString("batchid")).getOrElse(""),
                        "active" -> Boolean.box(Option(row.getBool("active")).getOrElse(false)),
                        "completedon" -> row.getTimestamp("completedon"),
                        "progress" -> Integer.valueOf(Option(row.getInt("progress")).getOrElse(0))
                    )
                })
    }
    /**
   * Generic method to read data from DB (Cassandra).
   *
   * @return
   */
    private def readFromDB(columns: Map[String, AnyRef], keySpace: String, table: String, metrics: Metrics): List[Row] = {
        val selectWhere: Select.Where = QueryBuilder.select().all()
        .from(keySpace, table).
        where()
        columns.map(col => {
        col._2 match {
            case value: List[Any] =>
            selectWhere.and(QueryBuilder.in(col._1, value.asJava))
            case _ =>
            selectWhere.and(QueryBuilder.eq(col._1, col._2))
        }
        })
        metrics.incCounter(config.dbReadCount)
        cassandraUtil.find(selectWhere.toString).asScala.toList

    }

    def readFromCache(key: String, metrics: Metrics): List[String] = {
        metrics.incCounter(config.cacheHitCount)
        val list = cache.getKeyMembers(key)
        if (CollectionUtils.isEmpty(list)) {
        metrics.incCounter(config.cacheMissCount)
        logger.info("Redis cache (smembers) not available for key: " + key)
        }
        list.asScala.toList
    }

    override def metricsList(): List[String] = {
        List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbWriteCount, config.dbReadCount, config.cacheHitCount, config.cacheMissCount)
    }


}
