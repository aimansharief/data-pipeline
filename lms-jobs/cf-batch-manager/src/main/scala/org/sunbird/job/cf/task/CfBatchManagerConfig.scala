package org.sunbird.job.cf.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.cf.domain.Event

class CfBatchManagerConfig(override val config: Config) extends BaseJobConfig(config, "cf-batch-manager") with Serializable {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  implicit val scalaMapTypeInfo: TypeInformation[Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaProgressionAuditTopic: String = if (config.hasPath("kafka.audit.progression.topic")) config.getString("kafka.audit.progression.topic") else "dev.user.enrollment.audit"
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")
  val kafkaFailedEventTopic: String = config.getString("kafka.output.failed.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val batchUpdaterParallelism: Int = config.getInt("task.batch.updater.parallelism")

  // Metric List
  val totalEventCount = "total-events-count"
  val failedEventCount = "failed-events-count"
  val processedEventCount = "processed-events-count"

  // Tags
  val auditEventOutputTagName = "audit-events"
  val auditEventOutputTag: OutputTag[String] = new OutputTag[String](auditEventOutputTagName)
  val failedEventOutputTagName = "failed-events"
  val failedEventOutputTag: OutputTag[String] = new OutputTag[String](failedEventOutputTagName)
  val batchUpdateOutputTagName = "batch-update-events"
  val batchUpdateOutputTag: OutputTag[Event] = new OutputTag[Event](batchUpdateOutputTagName)
  // Use a timestamp-based unique identifier with specific module prefix
  val userEnrollmentOutputTagName = s"cf-batch-mgr-ue"
  val userEnrollmentOutputTag: OutputTag[Event] = new OutputTag[Event](userEnrollmentOutputTagName)

  // Custom: CF batch cache side-output to trigger cache build in Redis
  val batchCacheOutputTagName = "cf-batch-cache"
  val batchCacheOutputTag: OutputTag[Event] = new OutputTag[Event](batchCacheOutputTagName)

  // constants
  val batchId = "batchId"
  val userId = "userId"
  val eData = "edata"
  val action = "action"
  val batchUpdaterFn = "batch-updater-fn"
  val batchUpdateAction = "batch-create"
  val userEnrollmentAction = "activity-enroll"
  val auditProgressionEid = "AUDIT"
  val auditProgressionType = "enrol-complete"
  val entranceExamOptionalThreshold: Int = if (config.hasPath("entrance.exam.optional.threshold.percent")) config.getInt("entrance.exam.optional.threshold.percent") else 100

  // Consumers
  val cfBatchManagerConsumer = "cf-batch-manager-consumer"
  val cfProgressionAuditConsumer = "cf-progress-audit-consumer"

  // Producers
  val cfBatchManagerProducer = "cf-batch-manager-audit-events-sink"
  val cfBatchManagerFailedEventProducer = "cf-batch-manager-failed-sink"

  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val lmsBasePath: String = config.getString("service.lms.basePath")
  val clBatchCreateRoute: String = config.getString("batch.create.endpoint.cl")
  val courseBatchCreateRoute: String = config.getString("batch.create.endpoint.course")
  val clEnrollRoute: String = config.getString("service.clEnroll.endpoint")
  val courseEnrollRoute: String = config.getString("service.courseEnroll.endpoint")
  val userEnrollKeyspace: String = config.getString("user-enrolments-cassandra.keyspace")
  val userEnrollTable: String = config.getString("user-enrolments-cassandra.table")
  // sb collection tracking keyspace/table (for statusmap lookups)
  val sbCollectionKeyspace: String = config.getString("sb-collection-cassandra.keyspace")
  val sbCollectionTable: String = config.getString("sb-collection-cassandra.table")

  // assessment aggregator keyspace/table
  val assessmentAggKeyspace: String = if (config.hasPath("assessment-aggregator-cassandra.keyspace")) config.getString("assessment-aggregator-cassandra.keyspace") else userEnrollKeyspace
  val assessmentAggTable: String = if (config.hasPath("assessment-aggregator-cassandra.table")) config.getString("assessment-aggregator-cassandra.table") else "assessment_aggregator"

  // Redis CF hierarchy cache (single redis block)
  val redisConnectionTimeout: Int = if (config.hasPath("redis.connection.timeout")) config.getInt("redis.connection.timeout") else 30000
  val cfHierarchyRedisHost: String = if (config.hasPath("redis.host")) config.getString("redis.host") else "localhost"
  val cfHierarchyRedisPort: Int = if (config.hasPath("redis.port")) config.getInt("redis.port") else 6379
  val cfHierarchyRedisDb: Int = if (config.hasPath("redis.database.index")) config.getInt("redis.database.index") else 6

  // Search Service Configuration
  val searchBasePath: String = getString("service.search.basePath", "http://search-service:9000")

}
