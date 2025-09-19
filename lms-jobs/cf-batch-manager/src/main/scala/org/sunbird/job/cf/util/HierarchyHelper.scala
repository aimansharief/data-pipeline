package org.sunbird.job.cf.util

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.core.JsonParser.Feature
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.CassandraUtil

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

  def isCollection(content: java.util.Map[String, AnyRef]): Boolean = {
    StringUtils.equalsIgnoreCase(content.getOrDefault("mimeType", "").asInstanceOf[String], "application/vnd.ekstep.content-collection")
  }

  def getOrComposeLeafNodes(hierarchy: java.util.Map[String, AnyRef], compose: Boolean = true): List[String] = {
    if (hierarchy.containsKey("leafNodes") && !compose)
      hierarchy.getOrDefault("leafNodes", java.util.Arrays.asList()).asInstanceOf[java.util.List[String]].asScala.toList
    else {
      val children = getChildren(hierarchy)
      val childCollections = children.asScala.filter(c => isCollection(c))
      val leafList = childCollections.flatMap(coll => getOrComposeLeafNodes(coll, compose = true)).toList
      val ids = children.asScala.filterNot(c => isCollection(c)).map(c => c.getOrDefault("identifier", "").asInstanceOf[String]).filter(id => StringUtils.isNotBlank(id))
      leafList ++ ids
    }
  }

  def getLeafNodes(identifier: String, hierarchy: java.util.Map[String, AnyRef]): Map[String, List[String]] = {
    val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
    val leafNodesMap = if (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection")) {
      val leafNodes = getOrComposeLeafNodes(hierarchy, compose = false)
      val map: Map[String, List[String]] = if (leafNodes.nonEmpty) Map() + (identifier -> leafNodes) else Map()
      val children = getChildren(hierarchy)
      val childLeafNodesMap = if (CollectionUtils.isNotEmpty(children)) {
        children.asScala.flatMap(child => {
          val childId = child.get("identifier").asInstanceOf[String]
          getLeafNodes(childId, child)
        }).toMap
      } else Map()
      map ++ childLeafNodesMap
    } else Map()
    leafNodesMap.filter(m => m._2.nonEmpty).toMap
  }

  def isOptional(content: java.util.Map[String, AnyRef]): Boolean = {
    val optionalMap = content.getOrDefault("relationalMetadata", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
    StringUtils.equalsIgnoreCase(optionalMap.getOrDefault("optional", "").toString, "true")
  }

  def getOrComposeOptionalNodes(hierarchy: java.util.Map[String, AnyRef], compose: Boolean = true): List[String] = {
    val children = getChildren(hierarchy)
    val ids = children.asScala.filter(c => isOptional(c)).map(c => c.getOrDefault("identifier", "").asInstanceOf[String]).filter(id => StringUtils.isNotBlank(id))
    val childCollections = children.asScala.filterNot(c => isOptional(c))
    val optionalList = childCollections.flatMap(coll => getOrComposeOptionalNodes(coll, compose = true)).toList
    optionalList ++ ids
  }

  def getOptionalNodes(identifier: String, hierarchy: java.util.Map[String, AnyRef]): Map[String, List[String]] = {
    val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
    val optionalNodesMap = if (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection")) {
      val optionalNodes = getOrComposeOptionalNodes(hierarchy, compose = false)
      val map: Map[String, List[String]] = if (optionalNodes.nonEmpty) Map() + (identifier -> optionalNodes) else Map()
      val children = getChildren(hierarchy)
      val childOptionalNodesMap = if (CollectionUtils.isNotEmpty(children)) {
        children.asScala.flatMap(child => {
          val childId = child.get("identifier").asInstanceOf[String]
          getOptionalNodes(childId, child)
        }).toMap
      } else Map()
      map ++ childOptionalNodesMap
    } else Map()
    optionalNodesMap.filter(m => m._2.nonEmpty).toMap
  }

  def getAncestors(identifier: String, hierarchy: java.util.Map[String, AnyRef], parents: List[String] = List()): Map[String, List[String]] = {
    val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
    val isColl = StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection")
    val ancestors = if (isColl) identifier :: parents else parents
    val ancestorsMap = if (isColl) {
      getChildren(hierarchy).asScala.map(child => {
        val childId = child.get("identifier").asInstanceOf[String]
        getAncestors(childId, child, ancestors)
      }).filter(m => m.nonEmpty).reduceOption((a, b) => {
        val grouped = (a.toSeq ++ b.toSeq).groupBy(_._1)
        grouped.mapValues(_.map(_._2).toList.flatten.distinct)
      }).getOrElse(Map())
    } else {
      Map(identifier -> parents)
    }
    ancestorsMap.filter(m => m._2.nonEmpty)
  }
}
