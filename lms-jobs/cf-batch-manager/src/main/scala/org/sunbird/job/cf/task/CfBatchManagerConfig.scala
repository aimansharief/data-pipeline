package org.sunbird.job.cf.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.cf.domain.Event

class CfBatchManagerConfig(override val config: Config) extends BaseJobConfig(config, "cf-batch-manager") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  implicit val scalaMapTypeInfo: TypeInformation[Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
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
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)
  val failedEventOutputTagName = "failed-events"
  val failedEventOutputTag: OutputTag[String] = OutputTag[String](failedEventOutputTagName)
  val batchUpdateOutputTagName = "batch-update-events"
  val batchUpdateOutputTag: OutputTag[Event] = OutputTag[Event](batchUpdateOutputTagName)
  val userEnrollmentOutputTagName = "user-enrollment-events"
  val userEnrollmentOutputTag: OutputTag[Event] = OutputTag[Event](userEnrollmentOutputTagName)

  // constants
  val batchId = "batchId"
  val userId = "userId"
  val eData = "edata"
  val action = "action"
  val batchUpdaterFn = "batch-updater-fn"
  val batchUpdateAction = "batch-create"
  val userEnrollmentAction = "user-enrollment"

  // Consumers
  val cfBatchManagerConsumer = "cf-batch-manager-consumer"

  // Producers
  val cfBatchManagerProducer = "cf-batch-manager-audit-events-sink"
  val cfBatchManagerFailedEventProducer = "cf-batch-manager-failed-sink"

  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

}
