package org.sunbird.job.spec

import java.util
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.cfprogress.domain.Event
import org.sunbird.job.cfprogress.task.{CFProgressUpdaterConfig, CFProgressUpdaterStreamTask}
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.dp.{BaseMetricsReporter, BaseTestSpec}
import org.scalatest.Ignore

import scala.collection.JavaConverters._

@Ignore
class CFProgressUpdaterTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CFProgressUpdaterConfig = new CFProgressUpdaterConfig(config)

  var cassandraUtil: CassandraUtil = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort, jobConfig.isMultiDCEnabled)
    val session = cassandraUtil.session

    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
    flinkCluster.after()
  }

  "CFProgressUpdater " should "process valid progress update events successfully" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new CFProgressUpdaterEventSource)
    new CFProgressUpdaterStreamTask(jobConfig, mockKafkaUtil).process()
    
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbWriteCount}").getValue() should be(2)

    // Verify data was written to Cassandra
    val selectQuery = s"SELECT * FROM ${jobConfig.courseKeyspace}.${jobConfig.courseEnrolmentsTable} WHERE userId = 'user123' AND activityId = 'activity456' AND batchId = 'batch789'"
    val rows = cassandraUtil.find(selectQuery)
    rows.size() should be(1)
    val row = rows.get(0)
    row.getDouble("progress") should be(75.5)
    row.getString("identifier") should be("progress_001")
    row.getString("activityType") should be("course")
  }

  "CFProgressUpdater " should "skip invalid events" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new CFProgressUpdaterInvalidEventSource)
    new CFProgressUpdaterStreamTask(jobConfig, mockKafkaUtil).process()
    
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(3)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(3)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbWriteCount}").getValue() should be(0)
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

}

class CFProgressUpdaterEventSource extends SourceFunction[Event] {
  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_EVENT), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_CF_PROGRESS_EVENT), 0, 11))
  }

  override def cancel() = {}
}

class CFProgressUpdaterInvalidEventSource extends SourceFunction[Event] {
  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_MISSING_USER_ID), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_INVALID_PROGRESS), 0, 11))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_UNSUPPORTED_ACTION), 0, 12))
  }

  override def cancel() = {}
}
