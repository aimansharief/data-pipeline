package org.sunbird.job.cfprogress.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.job.cfprogress.domain.Event
import org.sunbird.job.cfprogress.functions.CFProgressAggregatesFunction
import org.sunbird.dp.core.util.FlinkUtil


class CFProgressUpdaterStreamTask(config: CFProgressUpdaterConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)

    val processed = env.addSource(source).name(config.cfProgressConsumer)
      .uid(config.cfProgressConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CFProgressAggregatesFunction(config))
      .name("cf-progress-updater").uid("cf-progress-updater")
      .setParallelism(config.parallelism)

    // Side output: audit events -> Kafka string sink
    processed.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))

    // Side output: certificate issue events -> Kafka string sink
    processed.getSideOutput(config.certIssueEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.certIssueTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CFProgressUpdaterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("cf-progress-updater.conf").withFallback(ConfigFactory.systemEnvironment()))
    val cfProgressConfig = new CFProgressUpdaterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(cfProgressConfig)
    val task = new CFProgressUpdaterStreamTask(cfProgressConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
