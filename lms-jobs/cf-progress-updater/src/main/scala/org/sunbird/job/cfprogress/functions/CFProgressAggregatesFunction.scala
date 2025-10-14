package org.sunbird.job.cfprogress.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.job.cfprogress.domain.Event
import org.sunbird.job.cfprogress.task.CFProgressUpdaterConfig
import org.sunbird.dp.core.util.CassandraUtil
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

case class BatchMetadata(batchId: String, activityId: String, activityType: String)

class CFProgressAggregatesFunction(config: CFProgressUpdaterConfig, @transient var cassandraUtil: CassandraUtil = null)
                      (implicit val stringTypeInfo: TypeInformation[String])
    extends BaseProcessFunction[Event, String](config) {

    private[this] val logger = LoggerFactory.getLogger(classOf[CFProgressAggregatesFunction])
    lazy private val gson = new Gson()
    private val allowedActions = List("enrol-complete")
    
    private var cache: DataCache = _
    private val batchMetadataCache: scala.collection.mutable.Map[String, BatchMetadata] = scala.collection.mutable.Map.empty[String, BatchMetadata]

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        if (cassandraUtil == null) {
            cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
        }
        cache = new DataCache(config, new RedisConnect(config.redisHost, config.redisPort, config), config.nodeStore, List())
        cache.init()
    }

    override def close(): Unit = {
        cassandraUtil.close()
        cache.close()
        logger.info("Batch metadata cache final size: {} entries", batchMetadataCache.size.asInstanceOf[Object])
        batchMetadataCache.clear()
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
                    // Fetch batch metadata for all ancestors before computing parent progress
                    val batchMetadataMap = fetchBatchMetadata(ancestors, metrics)
                    logger.info("Fetched batch metadata for {} ancestors", batchMetadataMap.size.asInstanceOf[Object])
                    
                    // Extract batch metadata values and compute parent progress for each
                    val parentProgressList: List[Map[String, AnyRef]] = batchMetadataMap.values.map(batchMetadata => 
                        computeParentProgress(batchMetadata, event.userId, completedBatchIds, metrics)
                    ).toList
                    
                    // Update the parent progress in the database
                    updateParentProgress(parentProgressList, metrics)

                    //Generate AUDIT event for all the Activity completion and side-output to Kafka (gated by config)
                    if (config.activityProgressAuditEnabled) {
                        generateActivityCompletionAuditEvent(parentProgressList, event, context)(metrics)
                    }
                } else {
                    logger.warn(s"Ancestors is empty for key: ${event.batchId}-${config.ancestors} and userId: ${event.userId}")
                    metrics.incCounter(config.skippedEventCount)
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
        val columns = Map("userid" -> userId)
        Option(readFromDB(columns, config.courseKeyspace, config.courseEnrolmentsTable, metrics))
            .getOrElse(List.empty[Row])
            .filter(row => Option(row.getInt("status")).getOrElse(0) == 2)
            .map(row => {
                    Map(
                        "batchid" -> Option(row.getString("batchid")).getOrElse(""),
                        "active" -> Boolean.box(Option(row.getBool("active")).getOrElse(false)),
                        "completedon" -> row.getTimestamp("completedon"),
                        "progress" -> Integer.valueOf(Option(row.getInt("progress")).getOrElse(0))
                    )
                })
    }

    /**
     * Fetch batch metadata from in-memory cache or Cassandra materialized view
     * Handles partial cache hits by fetching only missing batch IDs from DB
     *
     * @param batchIds List of batch IDs to fetch metadata for
     * @param metrics  Metrics object to track DB operations
     * @return Map of batchId -> BatchMetadata containing activityid, activitytype, batchid
     */
    private def fetchBatchMetadata(batchIds: List[String], metrics: Metrics): Map[String, BatchMetadata] = {
        if (batchIds.isEmpty) {
            return Map.empty
        }

        // Separate batchIds into cached and not cached
        val (cachedBatchIds, uncachedBatchIds) = batchIds.partition(batchId => batchMetadataCache.contains(batchId))
        
        // Get cached metadata
        val cachedMetadata = cachedBatchIds.flatMap(batchId => 
            batchMetadataCache.get(batchId).map(metadata => batchId -> metadata)
        ).toMap
        
        logger.info("Batch metadata cache: {} hits, {} misses out of {} requested", 
                   cachedBatchIds.size.asInstanceOf[Object], 
                   uncachedBatchIds.size.asInstanceOf[Object], 
                   batchIds.size.asInstanceOf[Object])
        
        // Fetch uncached metadata from DB
        val dbMetadata = if (uncachedBatchIds.nonEmpty) {
            val columns = Map("batchid" -> uncachedBatchIds)
            val rows = Option(readFromDB(columns, config.collectionTrackingKeyspace, config.batchesByIdTable, metrics))
                .getOrElse(List.empty[Row])
            
            val fetchedMetadata = rows.map(row => {
                val batchId = Option(row.getString("batchid")).getOrElse("")
                val activityId = Option(row.getString("activityid")).getOrElse("")
                val activityType = Option(row.getString("activitytype")).getOrElse("")
                
                val metadata = BatchMetadata(batchId, activityId, activityType)
                
                // Update cache with newly fetched metadata
                batchMetadataCache.put(batchId, metadata)
                
                batchId -> metadata
            }).toMap
            
            logger.info("Fetched {} batch metadata records from DB and updated cache", 
                       fetchedMetadata.size.asInstanceOf[Object])
            
            fetchedMetadata
        } else {
            Map.empty[String, BatchMetadata]
        }
        
        // Combine cached and newly fetched metadata
        val combinedMetadata = cachedMetadata ++ dbMetadata
        
        logger.info("Returning {} batch metadata records (cached: {}, from DB: {})", 
                   combinedMetadata.size.asInstanceOf[Object], 
                   cachedMetadata.size.asInstanceOf[Object], 
                   dbMetadata.size.asInstanceOf[Object])
        
        combinedMetadata
    }

    /**
     * Fetch optional_batches for a given user/activity/batch from user_enrolments
     *
     * @param userId       The user ID
     * @param activityId   The activity ID
     * @param activityType The activity type
     * @param batchId      The batch ID (parent)
     * @param metrics      Metrics object to track DB operations
     * @return Set of optional batch IDs (empty if none)
     */
    private def fetchOptionalBatches(userId: String, activityId: String, activityType: String, batchId: String, metrics: Metrics): Set[String] = {
        val columns: Map[String, AnyRef] = Map(
            "userid" -> userId,
            "activityid" -> activityId,
            "activitytype" -> activityType,
            "batchid" -> batchId
        )

        val rows: List[Row] = Option(readFromDB(columns, config.collectionTrackingKeyspace, config.collectionEnrolmentsTable, metrics))
            .getOrElse(List.empty[Row])

        if (rows.isEmpty) {
            logger.info("No user_enrolments row found for optional_batches - userId: {}, activityId: {}, activityType: {}, batchId: {}", userId, activityId, activityType, batchId)
            Set.empty[String]
        } else {
            val row = rows.head
            try {
                val optional: java.util.List[String] = row.getList("optional_batches", classOf[String])
                if (optional == null) Set.empty[String] else optional.asScala.toSet
            } catch {
                case _: Exception =>
                    // If column doesn't exist or is different type, treat as empty
                    logger.warn("Failed to read optional_batches; treating as empty - userId: {}, activityId: {}, activityType: {}, batchId: {}", userId, activityId, activityType, batchId)
                    Set.empty[String]
            }
        }
    }

    /**
     * Compute progress for a parent batch based on completed child batches
     *
     * @param batchMetadata     Batch metadata containing batchId, activityId and activityType
     * @param userId            The user ID
     * @param completedBatchIds Set of completed batch IDs for the user
     * @param metrics           Metrics object to track operations
     * @return Map containing batchid, progress, userid, activityid, activitytype, and optionally completedon
     */
    private def computeParentProgress(batchMetadata: BatchMetadata, userId: String, completedBatchIds: Set[String], 
                                     metrics: Metrics): Map[String, AnyRef] = {
        val parentId = batchMetadata.batchId
        val activityId = batchMetadata.activityId
        val activityType = batchMetadata.activityType
        
        val leafNodeIds = readFromCache(key = s"${parentId}-${config.leafNodes}", metrics)
        if (leafNodeIds == null || leafNodeIds.isEmpty) {
            throw new RuntimeException(s"Cache missing or empty for key: ${parentId}-${config.leafNodes}. ParentId ${parentId} must have leafNodeIds.")
        }

        // Compute effective completion set: completed + optional
        val optionalBatchIds: Set[String] = fetchOptionalBatches(userId, activityId, activityType, parentId, metrics)
        val effectiveCompletedBatchIds: Set[String] = completedBatchIds.union(optionalBatchIds)

        // Compute intersection of leafNodeIds and effective completed batchIds
        val leafNodeIdSet = leafNodeIds.toSet
        val completedBatchIdsForParent = leafNodeIdSet.intersect(effectiveCompletedBatchIds).toList
        val completedCount = completedBatchIdsForParent.size
        val totalCount = leafNodeIdSet.size
        val progressPercentage = if (totalCount > 0) Math.ceil(completedCount * 100.0 / totalCount).toInt else 0
        
        logger.info("ParentId: {} | ActivityId: {} | ActivityType: {} | Total leafNodes: {} | Completed: {} | Progress: {}% | Completed batchIds: {}", 
                   parentId, activityId, activityType, totalCount.asInstanceOf[Object], completedCount.asInstanceOf[Object], 
                   progressPercentage.asInstanceOf[Object], completedBatchIdsForParent.mkString(", "))
        
        // Compose the result map
        val baseMap = Map(
            "batchid" -> parentId,
            "progress" -> Integer.valueOf(progressPercentage),
            "userid" -> userId,
            "activityid" -> activityId,
            "activitytype" -> activityType
        )
        
        if (progressPercentage == 100) {
            baseMap + ("completedon" -> new java.util.Date())
        } else {
            baseMap
        }
    }

    /**
     * Update parent progress records in the database
     *
     * @param parentProgressList List of parent progress maps to update
     * @param metrics            Metrics object to track operations
     */
    private def updateParentProgress(parentProgressList: List[Map[String, AnyRef]], metrics: Metrics): Unit = {
        if (parentProgressList.nonEmpty) {
            val updateQueries = parentProgressList.map(progressMap => getParentProgressUpdateQuery(progressMap))
            
            if (config.enableParentProgressUpdate) {
                // Execute the actual database update
                updateDB(config.thresholdBatchWriteSize, updateQueries, metrics)
                logger.info("Updated {} parent progress records", parentProgressList.size)
            } else {
                // Just print the queries without executing
                logger.info("Parent progress update is DISABLED. Would have executed {} queries:", parentProgressList.size.asInstanceOf[Object])
                updateQueries.foreach(query => {
                    logger.info("Query: {}", query.toString)
                })
            }
        }
    }

    /**
     * Generate CF audit events for entries with status=2 (completed)
     * edata.type = activity-complete
     * cdata includes Activity, activityType and CourseBatch identifiers
     */
    private def generateActivityCompletionAuditEvent(parentProgressList: List[Map[String, AnyRef]], sourceEvent: Event, context: ProcessFunction[Event, String]#Context)(metrics: Metrics) = {
        import org.sunbird.dp.core.util.JSONUtil
        import org.sunbird.job.cfprogress.domain.{TelemetryEvent, ActorObject, EventData, EventContext, EventObject}
        val completed = parentProgressList.filter(m => m.contains("completedon"))
        completed.map { m =>
            val userId = m("userid").toString
            val activityId = m("activityid").toString
            val activityType = m("activitytype").toString
            val batchId = m("batchid").toString

            val ctx = EventContext(
                channel = sourceEvent.readOrDefault[String]("context.channel", "in.sunbird"),
                env = sourceEvent.readOrDefault[String]("context.env", "CF"),
                sid = sourceEvent.readOrDefault[String]("context.sid", java.util.UUID.randomUUID().toString),
                did = sourceEvent.readOrDefault[String]("context.did", java.util.UUID.randomUUID().toString),
                cdata = Array(
                    Map("type" -> "Activity", "id" -> activityId),
                    Map("type" -> "ActivityType", "id" -> activityType),
                    Map("type" -> "Batch", "id" -> batchId)
                ).map(_.asJava)
            )

            val auditEvent = TelemetryEvent(
                actor = ActorObject(id = userId),
                edata = EventData(props = Array("completedon"), `type` = "activity-complete"),
                context = ctx,
                `object` = EventObject(id = userId, `type` = "User", rollup = Map("l1" -> activityId).asJava)
            )
            logger.info("audit event =>"+gson.toJson(auditEvent))
            context.output(config.auditEventOutputTag, gson.toJson(auditEvent))
        }
    }

    /**
     * Create an update query for a parent progress record
     *
     * @param progressMap Map containing userid, batchid, progress, activityid, activitytype, and optionally completedon
     * @return Update query
     */
    private def getParentProgressUpdateQuery(progressMap: Map[String, AnyRef]): Update.Where = {
        val userId = progressMap("userid").toString
        val batchId = progressMap("batchid").toString
        val progress = progressMap("progress").asInstanceOf[Integer].intValue()
        val activityId = progressMap("activityid").toString
        val activityType = progressMap("activitytype").toString
        
        logger.info("Creating update query for parent progress - userId: {}, batchId: {}, activityId: {}, activityType: {}, progress: {}", 
                   userId, batchId, activityId, activityType, progress.asInstanceOf[Object])
        
        val updateQuery = QueryBuilder.update(config.collectionTrackingKeyspace, config.collectionEnrolmentsTable)
            .`with`(QueryBuilder.set("progress", progress))
            .and(QueryBuilder.set("datetime", System.currentTimeMillis))
        
        // Add completedon if present (when progress is 100)
        if (progressMap.contains("completedon")) {
            updateQuery.and(QueryBuilder.set("completedon", progressMap("completedon")))
            updateQuery.and(QueryBuilder.set("status", 2)) // Status 2 means completed
        } else {
            updateQuery.and(QueryBuilder.set("status", 1)) // Status 1 means in progress
        }
        
        updateQuery
            .where(QueryBuilder.eq("userid", userId))
            .and(QueryBuilder.eq("activityid", activityId))
            .and(QueryBuilder.eq("activitytype", activityType))
            .and(QueryBuilder.eq("batchid", batchId))
    }

    /**
     * Method to update the database in batch format
     *
     * @param batchSize   Number of queries to batch together
     * @param queriesList List of update queries
     * @param metrics     Metrics object to track operations
     */
    private def updateDB(batchSize: Int, queriesList: List[Update.Where], metrics: Metrics): Unit = {
        val groupedQueries = queriesList.grouped(batchSize).toList
        groupedQueries.foreach(queries => {
            val cqlBatch = QueryBuilder.batch()
            queries.foreach(query => cqlBatch.add(query))
            val batchQuery = cqlBatch.toString
            logger.info("Executing batch upsert query: {}", batchQuery)
            val result = cassandraUtil.upsert(batchQuery)
            if (result) {
                metrics.incCounter(config.dbUpdateCount)
            } else {
                val msg = "Database update has failed: " + batchQuery
                logger.error(msg)
                throw new Exception(msg)
            }
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
        logger.info("selectWhere: " + selectWhere.toString)
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
        List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbWriteCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount, config.cacheMissCount)
    }


}
