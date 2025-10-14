package org.sunbird.job.cf.util

import org.slf4j.LoggerFactory
import org.sunbird.dp.core.cache.DataCache

import scala.collection.JavaConverters._

object CFCacheUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  case class CLNode(id: String, index: Int)
  case class CLStructure(id: String, index: Int, courseIds: List[String], levelExam: java.util.Map[String, AnyRef], entranceExam: java.util.Map[String, AnyRef]) {
    val levelExamId: String = {
      if (levelExam != null) {
        val v = levelExam.getOrDefault("collectionId", "").asInstanceOf[String]
        if (v != null && v.nonEmpty) v else null
      } else null
    }
    val entranceExamEnabled: Boolean = {
      if (entranceExam != null) {
        val v = entranceExam.getOrDefault("enabled", "").asInstanceOf[String]
        v != null && v.equalsIgnoreCase("Yes")
      } else false
    }
    val entranceExamId: String = {
      if (entranceExamEnabled && entranceExam != null) {
        val v = entranceExam.getOrDefault("collectionId", "").asInstanceOf[String]
        if (v != null && v.nonEmpty) v else null
      } else null
    }
  }

  private def metaKey(cfId: String): String = s"cf:$cfId:meta"
  private def clKey(cfId: String, clId: String): String = s"cf:$cfId:cl:$clId"

  def writeStructureToCache(cache: DataCache, cfId: String, orderedCLs: List[CLNode], structures: Map[String, CLStructure], enrollmentType: String): Unit = {
    if (cache == null) return
    try {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      
      // Write meta
      val metaData = Map(
        "enrollmentType" -> (if (enrollmentType == null) "" else enrollmentType),
        "clOrder" -> orderedCLs.map(_.id).mkString(","),
        "version" -> java.time.Instant.now().toString
      )
      cache.setWithRetry(metaKey(cfId), mapper.writeValueAsString(metaData.asJava))
      
      // Write each CL
      orderedCLs.foreach { clNode =>
        structures.get(clNode.id).foreach { s =>
          val clData = Map(
            "index" -> s.index.toString,
            "courses" -> s.courseIds.mkString(","),
            "entranceExamId" -> (if (s.entranceExamId != null) s.entranceExamId else ""),
            "levelExamId" -> (if (s.levelExamId != null) s.levelExamId else "")
          )
          cache.setWithRetry(clKey(cfId, s.id), mapper.writeValueAsString(clData.asJava))
        }
      }
      logger.info(s"RedisStructureWrite cf=$cfId meta+${orderedCLs.size}CLs written")
    } catch { case ex: Exception => logger.warn(s"Redis structure write failed cf=$cfId", ex) }
  }

  def readStructureFromCache(cache: DataCache, cfId: String): (List[CLNode], Map[String, CLStructure], String) = {
    if (cache == null) return null
    try {
      // Read meta
      val metaMap = cache.getWithRetry(metaKey(cfId))
      if (metaMap.isEmpty) return null
      
      val enrollmentType = metaMap.getOrElse("enrollmentType", "").toString
      val clOrderStr = metaMap.getOrElse("clOrder", "").toString
      if (clOrderStr.isEmpty) return null
      
      val clIds = clOrderStr.split(",").toList.filter(_.nonEmpty)
      val nodes = scala.collection.mutable.ListBuffer[CLNode]()
      val structures = scala.collection.mutable.Map[String, CLStructure]()
      
      // Read each CL
      clIds.foreach { clId =>
        val clMap = cache.getWithRetry(clKey(cfId, clId))
        if (clMap.nonEmpty) {
          val index = clMap.getOrElse("index", "0").toString.toInt
          val coursesStr = clMap.getOrElse("courses", "").toString
          val courses = if (coursesStr.nonEmpty) coursesStr.split(",").toList else Nil
          val entranceExamId = clMap.getOrElse("entranceexamid", "").toString
          val levelExamId = clMap.getOrElse("levelexamid", "").toString
          
          val levelExam = if (levelExamId.nonEmpty) { 
            val m = new java.util.HashMap[String, AnyRef](); m.put("collectionId", levelExamId); m 
          } else null
          val entranceExam = if (entranceExamId.nonEmpty) { 
            val m = new java.util.HashMap[String, AnyRef](); m.put("collectionId", entranceExamId); m.put("enabled", "Yes"); m 
          } else null
          
          nodes += CLNode(clId, index)
          structures += (clId -> CLStructure(clId, index, courses, levelExam, entranceExam))
        }
      }
      
      (nodes.toList.sortBy(_.index), structures.toMap, enrollmentType)
    } catch { case ex: Exception => logger.warn(s"Redis structure read failed cf=$cfId", ex); null }
  }

  def extractCLsAndCourses(hierarchy: java.util.Map[String, AnyRef], hierarchyHelper: HierarchyHelper): (List[CLNode], Map[String, CLStructure]) = {
    val clList = scala.collection.mutable.ListBuffer[CLNode]()
    val structures = scala.collection.mutable.Map[String, CLStructure]()
    
    hierarchyHelper.getChildren(hierarchy).asScala.foreach { child =>
      val primaryCategory = child.getOrDefault("primaryCategory", "").asInstanceOf[String]
      
      if (primaryCategory.equalsIgnoreCase("Competency Level")) {
        val levelId = child.getOrDefault("identifier", "").asInstanceOf[String]
        if (levelId != null && levelId.nonEmpty) {
          val index = child.get("index") match {
            case null => 0
            case idx => idx.asInstanceOf[Int]
          }
          
          val levelExam = if (child.containsKey("levelExam")) child.get("levelExam").asInstanceOf[java.util.Map[String, AnyRef]] else null
          val entranceExam = if (child.containsKey("entranceExam")) child.get("entranceExam").asInstanceOf[java.util.Map[String, AnyRef]] else null
          
          val levelChildren = child.getOrDefault("children", java.util.Collections.emptyList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
          val courseIds = levelChildren.asScala
            .filter(_.getOrDefault("primaryCategory", "").asInstanceOf[String].equalsIgnoreCase("Course"))
            .map(_.getOrDefault("identifier", "").asInstanceOf[String])
            .filter(id => id != null && id.nonEmpty)
            .toList
          
          val structure = CLStructure(levelId, index, courseIds, levelExam, entranceExam)
          structures += (levelId -> structure)
          clList += CLNode(levelId, index)
        }
      }
    }
    
    (clList.toList.sortBy(_.index), structures.toMap)
  }
}
