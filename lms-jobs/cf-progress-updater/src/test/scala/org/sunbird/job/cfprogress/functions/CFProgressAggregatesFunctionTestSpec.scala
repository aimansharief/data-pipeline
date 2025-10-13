package org.sunbird.job.cfprogress.functions

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.sunbird.job.cfprogress.domain.Event
import org.sunbird.job.cfprogress.task.CFProgressUpdaterConfig
import org.sunbird.job.fixture.EventFixture
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.dp.BaseTestSpec

import java.util

class CFProgressAggregatesFunctionTestSpec extends BaseTestSpec {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CFProgressUpdaterConfig = new CFProgressUpdaterConfig(config)
  
  private def setCassandraUtil(function: CFProgressAggregatesFunction, cassandraUtil: CassandraUtil): Unit = {
    val field = function.getClass.getDeclaredField("cassandraUtil")
    field.setAccessible(true)
    field.set(function, cassandraUtil)
  }

  "CFProgressAggregatesFunction" should "process valid event successfully" in {
    val mockCassandraUtil = mock[CassandraUtil]
    val cfProgressAggregatesFunction = new CFProgressAggregatesFunction(jobConfig)
    setCassandraUtil(cfProgressAggregatesFunction, mockCassandraUtil)
    val mockContext = mock[ProcessFunction[Event, String]#Context]
    val mockMetrics = mock[org.sunbird.dp.core.job.Metrics]

    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_PROGRESS_EVENT)
    val event = new Event(eventMap, 0, 10L)

    cfProgressAggregatesFunction.processElement(event, mockContext, mockMetrics)

    // TODO: Add verification once aggregation logic is implemented
    verify(mockMetrics, times(1)).incCounter(jobConfig.successEventCount)
    verify(mockMetrics, times(1)).incCounter(jobConfig.totalEventsCount)
  }

  "CFProgressAggregatesFunction" should "skip invalid event" in {
    val mockCassandraUtil = mock[CassandraUtil]
    val cfProgressAggregatesFunction = new CFProgressAggregatesFunction(jobConfig)
    setCassandraUtil(cfProgressAggregatesFunction, mockCassandraUtil)
    val mockContext = mock[ProcessFunction[Event, String]#Context]
    val mockMetrics = mock[org.sunbird.dp.core.job.Metrics]

    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.INVALID_EVENT_MISSING_USER_ID)
    val event = new Event(eventMap, 0, 10L)

    cfProgressAggregatesFunction.processElement(event, mockContext, mockMetrics)

    verify(mockMetrics, times(1)).incCounter(jobConfig.skippedEventCount)
    verify(mockMetrics, times(1)).incCounter(jobConfig.totalEventsCount)
    verify(mockMetrics, never()).incCounter(jobConfig.successEventCount)
  }


  "CFProgressAggregatesFunction" should "return correct metrics list" in {
    val cfProgressAggregatesFunction = new CFProgressAggregatesFunction(jobConfig)

    val metricsList = cfProgressAggregatesFunction.metricsList()

    metricsList should contain(jobConfig.successEventCount)
    metricsList should contain(jobConfig.failedEventCount)
    metricsList should contain(jobConfig.skippedEventCount)
    metricsList should contain(jobConfig.totalEventsCount)
    metricsList should contain(jobConfig.dbWriteCount)
    metricsList.size should be(8)
  }

  "CFProgressAggregatesFunction" should "handle close lifecycle method" in {
    val mockCassandraUtil = mock[CassandraUtil]
    val cfProgressAggregatesFunction = new CFProgressAggregatesFunction(jobConfig)
    setCassandraUtil(cfProgressAggregatesFunction, mockCassandraUtil)

    // Test close method
    cfProgressAggregatesFunction.close()
    verify(mockCassandraUtil, times(1)).close()
  }

  "CFProgressAggregatesFunction" should "process cf-progress-update action" in {
    val mockCassandraUtil = mock[CassandraUtil]
    val cfProgressAggregatesFunction = new CFProgressAggregatesFunction(jobConfig)
    setCassandraUtil(cfProgressAggregatesFunction, mockCassandraUtil)
    val mockContext = mock[ProcessFunction[Event, String]#Context]
    val mockMetrics = mock[org.sunbird.dp.core.job.Metrics]

    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.VALID_CF_PROGRESS_EVENT)
    val event = new Event(eventMap, 0, 10L)

    cfProgressAggregatesFunction.processElement(event, mockContext, mockMetrics)

    // TODO: Add verification once aggregation logic is implemented
    verify(mockMetrics, times(1)).incCounter(jobConfig.successEventCount)
    verify(mockMetrics, times(1)).incCounter(jobConfig.totalEventsCount)
  }

}
