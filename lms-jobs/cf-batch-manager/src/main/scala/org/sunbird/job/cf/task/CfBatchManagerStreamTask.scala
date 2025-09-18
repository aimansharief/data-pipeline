package org.sunbird.job.cf.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.functions.{BatchUpdaterFunction, CfEventRouter, UserEnrollmentFunction}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.FlinkUtil
import org.slf4j.LoggerFactory

import java.io.File


class CfBatchManagerStreamTask(config: CfBatchManagerConfig, kafkaConnector: FlinkKafkaConnector) {
  private[this] val logger = LoggerFactory.getLogger(classOf[CfBatchManagerStreamTask])
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)

    val routerStream = env.addSource(source).name(config.cfBatchManagerConsumer)
      .uid(config.cfBatchManagerConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CfEventRouter(config))
      .name("cf-batch-manager-router")
      .uid("cf-batch-manager-router")
      .setParallelism(config.kafkaConsumerParallelism)

    // Route to BatchUpdaterFunction
    val batchUpdateStream = routerStream.getSideOutput(config.batchUpdateOutputTag)
      .process(new BatchUpdaterFunction(config))
      .name(config.batchUpdaterFn)
      .uid(config.batchUpdaterFn)
      .setParallelism(config.batchUpdaterParallelism)

    // Route to UserEnrollmentFunction
    val userEnrollmentStream = routerStream.getSideOutput(config.userEnrollmentOutputTag)
      .process(new UserEnrollmentFunction(config))
      .name("user-enrollment-fn")
      .uid("user-enrollment-fn")
      .setParallelism(config.batchUpdaterParallelism)

    // Handle side outputs from both functions
    batchUpdateStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.cfBatchManagerProducer).uid(config.cfBatchManagerProducer)
    batchUpdateStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.cfBatchManagerFailedEventProducer).uid(config.cfBatchManagerFailedEventProducer)

    userEnrollmentStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name("user-enrollment-audit-producer").uid("user-enrollment-audit-producer")
    userEnrollmentStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name("user-enrollment-failed-producer").uid("user-enrollment-failed-producer")

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

