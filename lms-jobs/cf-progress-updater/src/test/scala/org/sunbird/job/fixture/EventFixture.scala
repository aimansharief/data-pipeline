package org.sunbird.job.fixture

object EventFixture {

  val VALID_PROGRESS_EVENT: String =
    """
      |{"actor":{"id":"CF Progress Processor", "type":"System"}, "eid":"BE_JOB_REQUEST", "edata":{"userId":"user123", "activityId":"activity456", "activityType":"competency-framework", "batchId":"batch789", "action":"progress-update", "progress":75.5, "identifier":"progress_001"}, "partition":0, "ets":"1.593769627322E12", "context":{"pdata":{"ver":1.0, "id":"org.ekstep.platform"}, "channel":"b00bc992ef25f1a9a8d63291e20efc8d", "env":"sunbirddev"}, "mid":"LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85", "object":{"ver":"1593769626118", "id":"progress_001"}}
      |""".stripMargin

  val VALID_CF_PROGRESS_EVENT: String =
    """
      |{"actor":{"id":"CF Progress Processor", "type":"System"}, "eid":"BE_JOB_REQUEST", "edata":{"userId":"user456", "activityId":"activity789", "activityType":"competency-framework", "batchId":"batch123", "action":"cf-progress-update", "progress":90.0, "identifier":"progress_002"}, "partition":0, "ets":"1.593769627322E12", "context":{"pdata":{"ver":1.0, "id":"org.ekstep.platform"}, "channel":"b00bc992ef25f1a9a8d63291e20efc8d", "env":"sunbirddev"}, "mid":"LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85", "object":{"ver":"1593769626118", "id":"progress_002"}}
      |""".stripMargin

  val INVALID_EVENT_MISSING_USER_ID: String =
    """
      |{"actor":{"id":"CF Progress Processor", "type":"System"}, "eid":"BE_JOB_REQUEST", "edata":{"activityId":"activity456", "activityType":"competency-framework", "batchId":"batch789", "action":"progress-update", "progress":75.5, "identifier":"progress_003"}, "partition":0, "ets":"1.593769627322E12", "context":{"pdata":{"ver":1.0, "id":"org.ekstep.platform"}, "channel":"b00bc992ef25f1a9a8d63291e20efc8d", "env":"sunbirddev"}, "mid":"LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85", "object":{"ver":"1593769626118", "id":"progress_003"}}
      |""".stripMargin

  val INVALID_EVENT_INVALID_PROGRESS: String =
    """
      |{"actor":{"id":"CF Progress Processor", "type":"System"}, "eid":"BE_JOB_REQUEST", "edata":{"userId":"user123", "activityId":"activity456", "activityType":"competency-framework", "batchId":"batch789", "action":"progress-update", "progress":150.0, "identifier":"progress_004"}, "partition":0, "ets":"1.593769627322E12", "context":{"pdata":{"ver":1.0, "id":"org.ekstep.platform"}, "channel":"b00bc992ef25f1a9a8d63291e20efc8d", "env":"sunbirddev"}, "mid":"LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85", "object":{"ver":"1593769626118", "id":"progress_004"}}
      |""".stripMargin

  val INVALID_EVENT_UNSUPPORTED_ACTION: String =
    """
      |{"actor":{"id":"CF Progress Processor", "type":"System"}, "eid":"BE_JOB_REQUEST", "edata":{"userId":"user123", "activityId":"activity456", "activityType":"competency-framework", "batchId":"batch789", "action":"unsupported-action", "progress":75.5, "identifier":"progress_005"}, "partition":0, "ets":"1.593769627322E12", "context":{"pdata":{"ver":1.0, "id":"org.ekstep.platform"}, "channel":"b00bc992ef25f1a9a8d63291e20efc8d", "env":"sunbirddev"}, "mid":"LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85", "object":{"ver":"1593769626118", "id":"progress_005"}}
      |""".stripMargin

}
