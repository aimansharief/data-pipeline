package org.sunbird.job.cf.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cf.domain.Event
import org.sunbird.job.cf.task.CfBatchManagerConfig
import org.sunbird.job.cf.util.HierarchyHelper
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class CFBatchCacheUpdate(config: CfBatchManagerConfig)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CFBatchCacheUpdate])

  @transient private var cassandraUtil: CassandraUtil = _
  @transient private var hierarchyHelper: HierarchyHelper = _
  @transient private var hierarchyCache: DataCache = _

  private val redisEnabled = true

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort, config.isMultiDCEnabled)
    hierarchyHelper = new HierarchyHelper(cassandraUtil, config.dbKeyspace, config.dbTable)
    if (redisEnabled) {
      try {
        hierarchyCache = new DataCache(config, new RedisConnect(config), config.cfHierarchyRedisDb, Nil)
        hierarchyCache.init()
        logger.info(s"Hierarchy DataCache initialised db=${config.cfHierarchyRedisDb}")
      } catch { case ex: Exception => logger.warn("Hierarchy DataCache init failed; proceeding without cache", ex) }
    }
    logger.info("CFBatchCacheUpdate opened")
  }

  override def close(): Unit = {
    logger.info("CFBatchCacheUpdate closing")
    try { if (hierarchyCache != null) hierarchyCache.close() } catch { case ex: Exception => logger.warn("Hierarchy cache close failed", ex) }
    try { if (cassandraUtil != null) cassandraUtil.close() } catch { case ex: Exception => logger.warn("Cassandra close failed", ex) }
    super.close()
  }

  override def metricsList(): List[String] = List(config.totalEventCount, config.failedEventCount, config.processedEventCount)

  private def storeDataInCache(rootId: String, suffix: String, dataMap: Map[String, AnyRef], cache: DataCache): Unit = {
    val finalSuffix = if (suffix != null && suffix.nonEmpty) s"-$suffix" else ""
    val finalPrefix = if (rootId != null && rootId.nonEmpty) s"$rootId:" else ""
    if (cache == null || dataMap == null || dataMap.isEmpty) return
    try {
      dataMap.foreach { case (k, v) =>
        v match {
          case list: List[_] =>
            val strList = list.asInstanceOf[List[String]]
            cache.createListWithRetry(finalPrefix + k + finalSuffix, strList)
          case s: String =>
            cache.setWithRetry(finalPrefix + k + finalSuffix, s)
          case other =>
            cache.setWithRetry(finalPrefix + k + finalSuffix, other.toString)
        }
      }
    } catch { case ex: Exception => logger.warn(s"storeDataInCache failed suffix=$suffix root=$rootId", ex) }
  }

  private def getLeafNodes(hierarchy: java.util.Map[String, AnyRef], baseBatchId: String): Map[String, List[String]] = {
    val result = scala.collection.mutable.Map[String, List[String]]()
    val children = hierarchyHelper.getChildren(hierarchy).asScala
    children.foreach { child =>
      val primaryCategory = child.getOrDefault("primaryCategory", "").asInstanceOf[String]
      if (primaryCategory.equalsIgnoreCase("Competency Level")) {
        val clId = Option(child.get("identifier")).map(_.toString).getOrElse("")
        if (clId.nonEmpty) {
          val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
          val courseIds = levelChildren.asScala
            .filter(_.getOrDefault("primaryCategory", "").asInstanceOf[String].equalsIgnoreCase("Course"))
            .map(_.getOrDefault("identifier", "").asInstanceOf[String])
            .filter(id => id != null && id.nonEmpty)
            .toList
          val listValues = courseIds.map(cid => s"$baseBatchId:$cid")
          result.put(clId, listValues)
        }
      }
    }
    result.toMap
  }

  private def getAllCoursesUnderCF(hierarchy: java.util.Map[String, AnyRef], baseBatchId: String): List[String] = {
    val all = scala.collection.mutable.ListBuffer[String]()
    val children = hierarchyHelper.getChildren(hierarchy).asScala
    children.foreach { child =>
      val primaryCategory = child.getOrDefault("primaryCategory", "").asInstanceOf[String]
      if (primaryCategory.equalsIgnoreCase("Competency Level")) {
        val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        val courseIds = levelChildren.asScala
          .filter(_.getOrDefault("primaryCategory", "").asInstanceOf[String].equalsIgnoreCase("Course"))
          .map(_.getOrDefault("identifier", "").asInstanceOf[String])
          .filter(id => id != null && id.nonEmpty)
          .toList
        all ++= courseIds.map(cid => s"$baseBatchId:$cid")
      }
    }
    all.toList.distinct
  }

  private def getAncestors(hierarchy: java.util.Map[String, AnyRef], cfId: String, baseBatchId: String): Map[String, List[String]] = {
    val result = scala.collection.mutable.Map[String, List[String]]()
    val children = hierarchyHelper.getChildren(hierarchy).asScala
    children.foreach { child =>
      val primaryCategory = child.getOrDefault("primaryCategory", "").asInstanceOf[String]
      if (primaryCategory.equalsIgnoreCase("Competency Level")) {
        val clId = Option(child.get("identifier")).map(_.toString).getOrElse("")
        if (clId.nonEmpty) {
          val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
          val courseIds = levelChildren.asScala
            .filter(_.getOrDefault("primaryCategory", "").asInstanceOf[String].equalsIgnoreCase("Course"))
            .map(_.getOrDefault("identifier", "").asInstanceOf[String])
            .filter(id => id != null && id.nonEmpty)
            .toList
          // Ancestors per course: include CL and CF-as-batch (no cfId)
          val ancestorValues = List(s"$baseBatchId:$clId", s"$baseBatchId")
          courseIds.foreach { cid => result.put(cid, ancestorValues) }
        }
      }
    }
    result.toMap
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventCount)
    try {
      // Check if action matches expected cache creation action
      if (!event.action.equals("cf-batch-cache-create")) {
        logger.warn(s"Unexpected action for cache update: ${event.action}, expected: cf-batch-cache-create")
        metrics.incCounter(config.failedEventCount)
        return
      }
      val cfId = Option(event.activityId).getOrElse("")
      val baseBatchId = Option(event.batchId).getOrElse("")
      if (cfId.nonEmpty && baseBatchId.nonEmpty) {
        val hierarchy = hierarchyHelper.getHierarchy(cfId)
        if (hierarchy != null && !hierarchy.isEmpty) {
          val leafNodesMap: Map[String, List[String]] = getLeafNodes(hierarchy, baseBatchId)
          val ancestorsMap: Map[String, List[String]] = getAncestors(hierarchy, cfId, baseBatchId)
          if (hierarchyCache != null) {
            storeDataInCache(baseBatchId, "leafnodes", leafNodesMap.asInstanceOf[Map[String, AnyRef]], hierarchyCache)
            storeDataInCache(baseBatchId, "ancestors", ancestorsMap.asInstanceOf[Map[String, AnyRef]], hierarchyCache)
            // Root-level leafnodes key: <batchId>-leafnodes, values are baseBatchId:courseId entries
            val allCourses = getAllCoursesUnderCF(hierarchy, baseBatchId)
            val rootKey = s"${baseBatchId}-leafnodes"
            if (allCourses.nonEmpty) hierarchyCache.createListWithRetry(rootKey, allCourses)
          }
          val clCount = leafNodesMap.size
          val totalCourses = leafNodesMap.values.map(_.size).sum
          logger.info(s"CFBatchCacheUpdate complete cf=$cfId clCount=$clCount totalCourses=$totalCourses baseBatch=$baseBatchId")
        } else {
          logger.warn(s"Empty hierarchy for cf=$cfId; skipping leafnodes/ancestors cache build")
        }
      } else {
        logger.warn(s"Missing cfId/baseBatchId; skipping cache build mid=${event.mid()} cf=$cfId base=$baseBatchId")
      }
      metrics.incCounter(config.processedEventCount)
    } catch {
      case ex: Exception =>
        logger.error(s"CFBatchCacheUpdate failed mid=${event.mid()} cf=${event.activityId}", ex)
        metrics.incCounter(config.failedEventCount)
    }
  }
}


