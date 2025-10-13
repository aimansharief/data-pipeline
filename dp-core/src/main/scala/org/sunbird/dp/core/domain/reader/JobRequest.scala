package org.sunbird.dp.core.domain.reader

import org.sunbird.dp.core.util.JSONUtil
import scala.collection.JavaConverters._

abstract class JobRequest(val map: java.util.Map[String, Any], val partition: Int, val offset: Long) extends Serializable {

  def getMap(): java.util.Map[String, Any] = map

  def getJson(): String = JSONUtil.serialize(getMap())

  def mid(): String = read[String](keyPath = EventsPath.MID_PATH).orNull

  def kafkaKey(): String = mid()

  def read[T](keyPath: String): Option[T] = try {
    val parentMap = lastParentMap(map, keyPath)
    Option(parentMap.readChild.orNull.asInstanceOf[T])
  } catch {
    case ex: Exception =>
      None
  }

  def readOrDefault[T](keyPath: String, defaultValue: T): T = {
    read(keyPath).getOrElse(defaultValue)
  }

  def readOrDefaultAsScalaList[T](keyPath: String): List[Map[String, AnyRef]] = {
    try {
      val javaList = readOrDefault[java.util.List[java.util.Map[String, AnyRef]]](keyPath, new java.util.ArrayList[java.util.Map[String, AnyRef]]())
      javaList.asScala.toList.map(_.asScala.toMap)
    } catch {
      case ex: Exception => List()
    }
  }

  def readOrDefaultAsScalaMap(keyPath: String): Map[String, AnyRef] = {
    try {
      val javaMap = readOrDefault[java.util.Map[String, AnyRef]](keyPath, new java.util.HashMap[String, AnyRef]())
      javaMap.asScala.toMap
    } catch {
      case ex: Exception => Map()
    }
  }

  @throws[JobRequestReaderException]
  def mustReadValue[T](keyPath: String): T = {
    read(keyPath).getOrElse({
      val mid = read("mid")
      throw new JobRequestReaderException(s"keyPath is not available in the $mid ")
    })
  }

  override def toString: String = "JobRequest {map=" + map + "}"

  private def lastParentMap(map: java.util.Map[String, Any], keyPath: String): ParentType = {
    try {
      var parent = map
      val keys = keyPath.split("\\.")
      val lastIndex = keys.length - 1
      if (keys.length > 1) {
        var i = 0
        while ( {
          i < lastIndex && parent != null
        }) {
          if (parent.isInstanceOf[java.util.Map[_, _]]) {
            parent = JSONUtil.deserialize[java.util.Map[String, Any]](JSONUtil.serialize(new ParentMap(parent, keys(i)).readChild.orNull))
          }
          i += 1
        }
      }
      val lastKeyInPath = keys(lastIndex)
      if (parent.isInstanceOf[java.util.Map[_, _]]) new ParentMap(parent, lastKeyInPath)
      else null
    } catch {
      case ex: Exception =>
        null
    }
  }
}

class JobRequestReaderException(val message: String) extends Exception(message) {}

