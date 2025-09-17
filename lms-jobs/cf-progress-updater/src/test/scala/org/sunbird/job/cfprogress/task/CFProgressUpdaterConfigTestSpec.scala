package org.sunbird.job.cfprogress.task

import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.spec.BaseTestSpec

class CFProgressUpdaterConfigTestSpec extends BaseTestSpec {

  "CFProgressUpdaterConfig" should "load configuration correctly" in {
    val config: Config = ConfigFactory.load("test.conf")
    val jobConfig = new CFProgressUpdaterConfig(config)

    jobConfig.jobName should be("cf-progress-updater")
    jobConfig.kafkaInputTopic should be("flink.cf.progress.input")
    jobConfig.kafkaConsumerParallelism should be(1)
    jobConfig.parallelism should be(2)
    jobConfig.dbKeyspace should be("cf_progress")
    jobConfig.dbTable should be("user_cf_progress")
    jobConfig.dbHost should be("localhost")
    jobConfig.dbPort should be(9142)
  }

  "CFProgressUpdaterConfig" should "have correct metric names" in {
    val config: Config = ConfigFactory.load("test.conf")
    val jobConfig = new CFProgressUpdaterConfig(config)

    jobConfig.totalEventsCount should be("total-events-count")
    jobConfig.successEventCount should be("success-events-count")
    jobConfig.failedEventCount should be("failed-events-count")
    jobConfig.skippedEventCount should be("skipped-event-count")
    jobConfig.dbWriteCount should be("db-write-count")
  }

  "CFProgressUpdaterConfig" should "have correct consumer name" in {
    val config: Config = ConfigFactory.load("test.conf")
    val jobConfig = new CFProgressUpdaterConfig(config)

    jobConfig.cfProgressConsumer should be("cf-progress-consumer")
  }

  "CFProgressUpdaterConfig" should "have correct primary key configuration" in {
    val config: Config = ConfigFactory.load("test.conf")
    val jobConfig = new CFProgressUpdaterConfig(config)

    jobConfig.progressPrimaryKey should contain("userId")
    jobConfig.progressPrimaryKey should contain("activityId")
    jobConfig.progressPrimaryKey should contain("batchId")
    jobConfig.progressPrimaryKey.size should be(3)
  }

}
