package org.sunbird.job.cf.util

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.core.JsonParser.Feature
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.util.CassandraUtil

import scala.collection.JavaConverters._

class HierarchyHelper(@transient private val cassandraUtil: CassandraUtil,
                      private val keyspace: String,
                      private val table: String) extends Serializable {

  private[this] val logger = LoggerFactory.getLogger(classOf[HierarchyHelper])
  private val mapper: ObjectMapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true)

  def getHierarchy(identifier: String): java.util.Map[String, AnyRef] = {
    try {
      val json = readHierarchyFromDb(identifier)
      if (StringUtils.isNotBlank(json)) mapper.readValue(json, classOf[java.util.Map[String, AnyRef]])
      else new java.util.HashMap[String, AnyRef]()
    } catch {
      case t: Throwable =>
        logger.error(s"Failed to get hierarchy for id=$identifier", t)
        new java.util.HashMap[String, AnyRef]()
    }
  }

  /**
    * Fetch hierarchy using Redis cache when available, else read from DB and populate cache.
    * - Cache key: activityId
    * - Cache value: raw JSON string of the hierarchy
    */
  def getHierarchyWithCache(activityId: String, cache: DataCache): java.util.Map[String, AnyRef] = {
    if (cache != null && StringUtils.isNotBlank(activityId)) {
      try {
        val cached = cache.getWithRetryCasePreserved(activityId)
        if (cached != null && !cached.isEmpty) {
          val jMap = new java.util.HashMap[String, AnyRef]()
          cached.foreach { case (k, v) => jMap.put(k, v.asInstanceOf[AnyRef]) }
          return jMap
        }
      } catch { case ex: Exception => logger.warn(s"Hierarchy cache read failed for $activityId", ex) }
    }
    val hierarchy = getHierarchy(activityId)
    if (hierarchy != null && !hierarchy.isEmpty && cache != null) {
      try {
        val json = mapper.writeValueAsString(hierarchy)
        cache.setWithRetry(activityId, json)
      } catch { case ex: Exception => logger.warn(s"Hierarchy cache write failed for $activityId", ex) }
    }
    hierarchy
  }

  def readHierarchyFromDb(identifier: String): String = {
    if (StringUtils.isBlank(keyspace) || StringUtils.isBlank(table)) {
      logger.warn("HierarchyHelper: keyspace/table not configured; skipping hierarchy read")
      return ""
    }
    val selectQuery = QueryBuilder.select().column("hierarchy").from(keyspace, table)
    selectQuery.where.and(QueryBuilder.eq("identifier", identifier))
    val rows = cassandraUtil.find(selectQuery.toString)
    if (CollectionUtils.isNotEmpty(rows)) rows.asScala.head.getObject("hierarchy").asInstanceOf[String] else ""
  }

  def getChildren(hierarchy: java.util.Map[String, AnyRef]): java.util.List[java.util.Map[String, AnyRef]] = {
    val children = hierarchy.getOrDefault("children", java.util.Arrays.asList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    if (CollectionUtils.isEmpty(children)) List().asJava else children
  }

}
