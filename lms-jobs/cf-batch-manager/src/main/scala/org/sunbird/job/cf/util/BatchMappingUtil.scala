package org.sunbird.job.cf.util

import org.slf4j.LoggerFactory
import org.sunbird.dp.core.cache.DataCache

object BatchMappingUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  def storeBatchMapping(cache: DataCache, batchId: String, id: String, tpe: String, ttlSeconds: Option[Int] = None): Unit = {
    if (cache == null) return
    if (batchId == null || batchId.isEmpty || id == null || id.isEmpty || tpe == null || tpe.isEmpty) return
    val normalizedType = tpe match {
      case s if s.equalsIgnoreCase("Competency Framework") => "Competency Framework"
      case s if s.equalsIgnoreCase("Competency Level") => "Competency Level"
      case s if s.equalsIgnoreCase("Course") => "Course"
      case other => other
    }
    val value = s"""{"id":"$id","type":"$normalizedType"}"""
    try {
      ttlSeconds match {
        case Some(ttl) if ttl > 0 => cache.setWithExpiry(batchId, value, ttl)
        case _ => cache.setWithRetry(batchId, value)
      }
    } catch { case ex: Exception => logger.warn(s"BatchMapping store failed batch=$batchId id=$id type=$normalizedType ttl=${ttlSeconds.getOrElse(0)}", ex) }
  }

  def readBatchMapping(cache: DataCache, batchId: String): Option[(String, String)] = {
    if (cache == null || batchId == null || batchId.isEmpty) return None
    try {
      val map = cache.getWithRetry(batchId)
      if (map != null && map.nonEmpty) {
        val id = map.get("id").map(_.toString).getOrElse("")
        val t = map.get("type").map(_.toString).getOrElse("")
        if (id.nonEmpty && t.nonEmpty) return Some(id -> t)
      }
    } catch { case ex: Exception => logger.warn(s"BatchMapping read failed batch=$batchId", ex) }
    None
  }
}
