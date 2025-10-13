package org.sunbird.job.spec

import java.util
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.cfprogress.domain.Event
import org.sunbird.job.cfprogress.task.{CFProgressUpdaterConfig, CFProgressUpdaterStreamTask}
import org.sunbird.dp.core.util.JSONUtil
import org.sunbird.dp.BaseTestSpec

import scala.collection.JavaConverters._

class CFProgressUpdaterTaskTestSpecFast extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CFProgressUpdaterConfig = new CFProgressUpdaterConfig(config)

  "CFProgressUpdaterTask" should "create stream task successfully" in {
    val task = new CFProgressUpdaterStreamTask(jobConfig, mockKafkaUtil)
    task should not be null
  }

  "CFProgressUpdaterTask" should "have correct configuration" in {
    jobConfig.jobName should be("cf-progress-updater")
    jobConfig.kafkaInputTopic should be("flink.cf.progress.input")
    jobConfig.kafkaConsumerParallelism should be(1)
    jobConfig.parallelism should be(2)
  }

  "CFProgressUpdaterEventSourceFast" should "generate events correctly" in {
    val source = new CFProgressUpdaterEventSourceFast
    source should not be null
  }

  "CFProgressUpdaterInvalidEventSourceFast" should "generate invalid events correctly" in {
    val source = new CFProgressUpdaterInvalidEventSourceFast
    source should not be null
  }

  "Event parsing" should "work correctly for valid events" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_EVENT)
    val event = new Event(eventMap, 0, 10L)
    
    event.identifier should be("progress_001")
    event.action should be("progress-update")
    event.userId should be("user123")
    event.activityId should be("activity456")
    event.activityType should be("competency-framework")
    event.batchId should be("batch789")
    event.progress should be(75.5)
  }

  "Event parsing" should "work correctly for invalid events" in {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_MISSING_USER_ID)
    val event = new Event(eventMap, 0, 10L)
    
    event.userId should be("")
    event.activityId should be("activity456")
    event.activityType should be("competency-framework")
  }

}

class CFProgressUpdaterEventSourceFast extends org.apache.flink.streaming.api.functions.source.SourceFunction[Event] {
  override def run(ctx: org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_EVENT), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_CF_PROGRESS_EVENT), 0, 11))
  }

  override def cancel() = {}
}

class CFProgressUpdaterInvalidEventSourceFast extends org.apache.flink.streaming.api.functions.source.SourceFunction[Event] {
  override def run(ctx: org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_MISSING_USER_ID), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_INVALID_PROGRESS), 0, 11))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_UNSUPPORTED_ACTION), 0, 12))
  }

  override def cancel() = {}
}
