package org.sunbird.job.cfprogress.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class CFProgressUpdaterConfig(override val config: Config) extends BaseJobConfig(config, "cf-progress-updater") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbWriteCount = "db-write-count"
  val dbReadCount = "db-read-count"
  val cacheHitCount = "cache-hit-count"
  val cacheMissCount = "cache-miss-count"

  // Consumers
  val cfProgressConsumer = "cf-progress-consumer"

  // Cassandra Configurations
  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val progressPrimaryKey: List[String] = List("userId", "activityId", "batchId")

  // User Enrolments Configuration
  val courseKeyspace: String = if (config.hasPath("course-cassandra.keyspace")) config.getString("course-cassandra.keyspace") else "sunbird_courses"
  val courseEnrolmentsTable: String = if (config.hasPath("course-cassandra.user-enrolments.table")) config.getString("course-cassandra.user-enrolments.table") else "user_enrolments"

  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.relationCache.id")

}
