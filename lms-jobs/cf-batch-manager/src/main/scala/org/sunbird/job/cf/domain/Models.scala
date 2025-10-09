package org.sunbird.job.cf.domain

import java.util
import java.util.{Date, UUID}

import scala.collection.JavaConverters._

case class BatchData(batchId: String, userId: String, status: String, createdOn: Date = new Date())

case class BatchUpdateEvent(batchId: String, userId: String, action: String, data: Map[String, AnyRef])
