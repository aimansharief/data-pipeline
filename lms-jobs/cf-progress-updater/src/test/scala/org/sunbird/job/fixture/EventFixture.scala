package org.sunbird.job.fixture

object EventFixture {

  // Valid event with complete enrolment
  val VALID_ENROL_COMPLETE_EVENT: String =
    """
      |{"actor":{"id":"72f74abc-ff01-4b0b-9bb3-7c4f4cca8fba","type":"User"},"eid":"AUDIT","edata":{"props":["status","completedon"],"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","sid":"8b7284f5-4007-4fc6-ad73-dd0f0432db04","did":"c16f809b-4a63-4e98-a83d-76e536753ec9","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform","pid":"course-progress-updater"},"cdata":[{"type":"CourseBatch","id":"0144172664199004160:do_21441672239907635218"},{"type":"Course","id":"do_21441672239907635218"}]},"mid":"LP.AUDIT.792ef87a-2e92-4275-a8f4-b5cab978fa5d","object":{"id":"72f74abc-ff01-4b0b-9bb3-7c4f4cca8fba","type":"User","rollup":{"l1":"do_21441672239907635218"}},"tags":[]}
      |""".stripMargin

  // Valid event with progress update
  val VALID_PROGRESS_UPDATE_EVENT: String =
    """
      |{"actor":{"id":"user-123-456-789","type":"User"},"eid":"AUDIT","edata":{"type":"progress-update","progress":75.5},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-001"},{"type":"Course","id":"course-001"}]},"mid":"LP.AUDIT.progress-update-001","object":{"id":"user-123-456-789","type":"User","rollup":{"l1":"course-001"}},"tags":[]}
      |""".stripMargin

  // Valid event with only actor.id for userId
  val VALID_EVENT_WITH_ACTOR_ID: String =
    """
      |{"actor":{"id":"actor-user-001","type":"User"},"eid":"AUDIT","edata":{"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-002"},{"type":"Course","id":"course-002"}]},"mid":"LP.AUDIT.actor-test-001","object":{"id":"object-user-001","type":"User","rollup":{"l1":"course-002"}},"tags":[]}
      |""".stripMargin

  // Valid event with object.id fallback for userId (no actor.id)
  val VALID_EVENT_WITH_OBJECT_ID_FALLBACK: String =
    """
      |{"actor":{"type":"User"},"eid":"AUDIT","edata":{"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-003"},{"type":"Course","id":"course-003"}]},"mid":"LP.AUDIT.object-fallback-001","object":{"id":"fallback-user-001","type":"User","rollup":{"l1":"course-003"}},"tags":[]}
      |""".stripMargin

  // Valid event with multiple cdata entries (testing cdata parsing)
  val VALID_EVENT_WITH_MULTIPLE_CDATA: String =
    """
      |{"actor":{"id":"user-multi-cdata","type":"User"},"eid":"AUDIT","edata":{"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"Program","id":"program-001"},{"type":"CourseBatch","id":"batch-multi-001"},{"type":"Course","id":"course-multi-001"}]},"mid":"LP.AUDIT.multi-cdata-001","object":{"id":"user-multi-cdata","type":"User","rollup":{"l1":"course-multi-001"}},"tags":[]}
      |""".stripMargin

  // Valid event with missing cdata (fallback to object.rollup.l1)
  val VALID_EVENT_WITH_NO_CDATA: String =
    """
      |{"actor":{"id":"user-no-cdata","type":"User"},"eid":"AUDIT","edata":{"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"}},"mid":"LP.AUDIT.no-cdata-001","object":{"id":"user-no-cdata","type":"User","rollup":{"l1":"course-from-rollup"}},"tags":[]}
      |""".stripMargin

  // Valid event with zero progress
  val VALID_EVENT_WITH_ZERO_PROGRESS: String =
    """
      |{"actor":{"id":"user-zero-progress","type":"User"},"eid":"AUDIT","edata":{"type":"progress-update","progress":0.0},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-zero"},{"type":"Course","id":"course-zero"}]},"mid":"LP.AUDIT.zero-progress-001","object":{"id":"user-zero-progress","type":"User","rollup":{"l1":"course-zero"}},"tags":[]}
      |""".stripMargin

  // Valid event with 100% progress
  val VALID_EVENT_WITH_FULL_PROGRESS: String =
    """
      |{"actor":{"id":"user-full-progress","type":"User"},"eid":"AUDIT","edata":{"type":"progress-update","progress":100.0},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-full"},{"type":"Course","id":"course-full"}]},"mid":"LP.AUDIT.full-progress-001","object":{"id":"user-full-progress","type":"User","rollup":{"l1":"course-full"}},"tags":[]}
      |""".stripMargin

  // Valid event with decimal progress
  val VALID_EVENT_WITH_DECIMAL_PROGRESS: String =
    """
      |{"actor":{"id":"user-decimal-progress","type":"User"},"eid":"AUDIT","edata":{"type":"progress-update","progress":45.67},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-decimal"},{"type":"Course","id":"course-decimal"}]},"mid":"LP.AUDIT.decimal-progress-001","object":{"id":"user-decimal-progress","type":"User","rollup":{"l1":"course-decimal"}},"tags":[]}
      |""".stripMargin

  // Valid event with empty cdata array
  val VALID_EVENT_WITH_EMPTY_CDATA: String =
    """
      |{"actor":{"id":"user-empty-cdata","type":"User"},"eid":"AUDIT","edata":{"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[]},"mid":"LP.AUDIT.empty-cdata-001","object":{"id":"user-empty-cdata","type":"User","rollup":{"l1":"course-from-empty-cdata-rollup"}},"tags":[]}
      |""".stripMargin

  // Invalid event - missing userId (both actor.id and object.id are blank/missing)
  val INVALID_EVENT_MISSING_USER_ID: String =
    """
      |{"actor":{"type":"User"},"eid":"AUDIT","edata":{"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-no-user"},{"type":"Course","id":"course-no-user"}]},"mid":"LP.AUDIT.no-user-001","object":{"type":"User","rollup":{"l1":"course-no-user"}},"tags":[]}
      |""".stripMargin

  // Invalid event - wrong eid (not AUDIT)
  val INVALID_EVENT_WRONG_EID: String =
    """
      |{"actor":{"id":"user-wrong-eid","type":"User"},"eid":"BE_JOB_REQUEST","edata":{"type":"enrol-complete"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-wrong-eid"},{"type":"Course","id":"course-wrong-eid"}]},"mid":"LP.AUDIT.wrong-eid-001","object":{"id":"user-wrong-eid","type":"User","rollup":{"l1":"course-wrong-eid"}},"tags":[]}
      |""".stripMargin

  // Invalid event - unsupported action
  val INVALID_EVENT_UNSUPPORTED_ACTION: String =
    """
      |{"actor":{"id":"user-unsupported","type":"User"},"eid":"AUDIT","edata":{"type":"unsupported-action"},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-unsupported"},{"type":"Course","id":"course-unsupported"}]},"mid":"LP.AUDIT.unsupported-001","object":{"id":"user-unsupported","type":"User","rollup":{"l1":"course-unsupported"}},"tags":[]}
      |""".stripMargin

  // ============== LEGACY FIXTURES (for backward compatibility with other tests) ==============

  // Legacy: Valid progress event (kept for backward compatibility)
  val VALID_PROGRESS_EVENT: String = VALID_PROGRESS_UPDATE_EVENT

  // Legacy: Valid CF progress event (kept for backward compatibility)
  val VALID_CF_PROGRESS_EVENT: String =
    """
      |{"actor":{"id":"user-cf-progress","type":"User"},"eid":"AUDIT","edata":{"type":"cf-progress-update","progress":90.0},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-cf-001"},{"type":"Course","id":"course-cf-001"}]},"mid":"LP.AUDIT.cf-progress-update-001","object":{"id":"user-cf-progress","type":"User","rollup":{"l1":"course-cf-001"}},"tags":[]}
      |""".stripMargin

  // Legacy: Invalid event with invalid progress (kept for backward compatibility)
  val INVALID_EVENT_INVALID_PROGRESS: String =
    """
      |{"actor":{"id":"user-invalid-progress","type":"User"},"eid":"AUDIT","edata":{"type":"progress-update","progress":150.0},"ver":"3.0","syncts":1759986363140,"ets":1759986363140,"context":{"channel":"in.sunbird","env":"Course","pdata":{"ver":"3.0","id":"org.sunbird.learning.platform"},"cdata":[{"type":"CourseBatch","id":"batch-invalid"},{"type":"Course","id":"course-invalid"}]},"mid":"LP.AUDIT.invalid-progress-001","object":{"id":"user-invalid-progress","type":"User","rollup":{"l1":"course-invalid"}},"tags":[]}
      |""".stripMargin

}
