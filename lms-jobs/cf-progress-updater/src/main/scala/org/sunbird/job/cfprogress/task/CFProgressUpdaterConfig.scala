package org.sunbird.job.cfprogress.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.dp.core.job.BaseJobConfig
import org.apache.flink.streaming.api.scala.OutputTag

class CFProgressUpdaterConfig(override val config: Config) extends BaseJobConfig(config, "cf-progress-updater") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditEventTopic: String = if (config.hasPath("kafka.output.audit.topic")) config.getString("kafka.output.audit.topic") else "dev.cf.progress.audit"
  val certIssueTopic: String = config.getString("kafka.output.certissue.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbWriteCount = "db-write-count"
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val cacheHitCount = "cache-hit-count"
  val cacheMissCount = "cache-miss-count"

  // Consumers
  val cfProgressConsumer = "cf-progress-consumer"

  // Cassandra Configurations
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val progressPrimaryKey: List[String] = List("userId", "activityId", "batchId")

  // User Enrolments Configuration
  val courseKeyspace: String = if (config.hasPath("course-cassandra.keyspace")) config.getString("course-cassandra.keyspace") else "sunbird_courses"
  val courseEnrolmentsTable: String = if (config.hasPath("course-cassandra.user-enrolments.table")) config.getString("course-cassandra.user-enrolments.table") else "user_enrolments"
  val collectionTrackingKeyspace: String = if (config.hasPath("collection-tracking-cassandra.keyspace")) config.getString("collection-tracking-cassandra.keyspace") else "sb_collection_tracking"
  val collectionEnrolmentsTable: String = if (config.hasPath("collection-tracking-cassandra.user-enrolments.table")) config.getString("collection-tracking-cassandra.user-enrolments.table") else "user_enrolments"
  val batchesByIdTable: String = if (config.hasPath("collection-tracking-cassandra.batches-by-id.table")) config.getString("collection-tracking-cassandra.batches-by-id.table") else "batches_by_id"

  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.relationCache.id")

  // Constants for cache keys
  val leafNodes = "leafnodes"
  val ancestors = "ancestors"

  // Batch write configuration
  val thresholdBatchWriteSize: Int = if (config.hasPath("threshold.batch.write.size")) config.getInt("threshold.batch.write.size") else 10
  val enableParentProgressUpdate: Boolean = if (config.hasPath("enable.parent.progress.update")) config.getBoolean("enable.parent.progress.update") else false

  // Hardcoded values for parent progress tracking
  val parentActivityId: String = "collection-framework"
  val parentActivityType: String = "CollectionFramework"

  // Side output tags
  val auditEventOutputTagName: String = "cf-progress-audit-events"
  val auditEventOutputTag: OutputTag[String] = new OutputTag[String](auditEventOutputTagName)

  // Feature flags
  val activityProgressAuditEnabled: Boolean = if (config.hasPath("activity.progress.audit.enabled")) config.getBoolean("activity.progress.audit.enabled") else false

}
