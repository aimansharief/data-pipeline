package org.sunbird.job.cf.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.functions.{BatchUpdaterFunction, CfEventRouter, UserEnrollmentFunction}
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.slf4j.LoggerFactory

import java.io.File


class CfBatchManagerStreamTask(config: CfBatchManagerConfig, kafkaConnector: FlinkKafkaConnector) {
  private[this] val logger = LoggerFactory.getLogger(classOf[CfBatchManagerStreamTask])
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val primarySource = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val progressionSource = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaProgressionAuditTopic)

    // Extract constants to avoid serialization issues with config object
    val batchUpdateAction = config.batchUpdateAction
    val userEnrollmentAction = config.userEnrollmentAction

    // Process events directly without router to avoid side output issues
    val batchUpdateStream = env.addSource(primarySource).name(config.cfBatchManagerConsumer)
      .uid(config.cfBatchManagerConsumer).setParallelism(config.kafkaConsumerParallelism)
      .filter(_.action == batchUpdateAction)
      .process(new BatchUpdaterFunction(config))
      .name(config.batchUpdaterFn)
      .uid(config.batchUpdaterFn)
      .setParallelism(config.batchUpdaterParallelism)

    val userEnrollmentStream = env.addSource(primarySource).name(s"${config.cfBatchManagerConsumer}-ue")
      .uid(s"${config.cfBatchManagerConsumer}-ue").setParallelism(config.kafkaConsumerParallelism)
      .filter(event => event.action == userEnrollmentAction)
      .process(new UserEnrollmentFunction(config))
      .name("user-enrollment-fn")
      .uid("user-enrollment-fn")
      .setParallelism(config.batchUpdaterParallelism)

    // New progression audit stream (AUDIT enrol-complete events) directly to user enrollment (bypasses CfEventRouter)
    val auditProgressionStream = env.addSource(progressionSource)
      .name(config.cfProgressionAuditConsumer)
      .uid(config.cfProgressionAuditConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .process(new UserEnrollmentFunction(config))
      .name("user-enrollment-fn-audit-progression")
      .uid("user-enrollment-fn-audit-progression")
      .setParallelism(config.batchUpdaterParallelism)

    // Side outputs
    batchUpdateStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.cfBatchManagerProducer).uid(config.cfBatchManagerProducer)
    batchUpdateStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.cfBatchManagerFailedEventProducer).uid(config.cfBatchManagerFailedEventProducer)

    // Trigger CF batch cache builder from BatchUpdater side-output
    batchUpdateStream.getSideOutput(config.batchCacheOutputTag)
      .process(new org.sunbird.job.cf.functions.CFBatchCacheUpdate(config))
      .name("cf-batch-cache-fn").uid("cf-batch-cache-fn")
      .setParallelism(config.batchUpdaterParallelism)

    userEnrollmentStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name("user-enrollment-audit-producer").uid("user-enrollment-audit-producer")
    userEnrollmentStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name("user-enrollment-failed-producer").uid("user-enrollment-failed-producer")

    auditProgressionStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name("audit-progression-audit-producer").uid("audit-progression-audit-producer")
    auditProgressionStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name("audit-progression-failed-producer").uid("audit-progression-failed-producer")

    logger.info(s"Starting Flink job: ${config.jobName}")
    env.execute(config.jobName)
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CfBatchManagerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("cf-batch-manager.conf").withFallback(ConfigFactory.systemEnvironment()))
    val cfBatchManager = new CfBatchManagerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(cfBatchManager)
    val task = new CfBatchManagerStreamTask(cfBatchManager, kafkaUtil)
    task.process()
  }

}

