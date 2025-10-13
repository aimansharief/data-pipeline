package org.sunbird.dp.core.domain.reader

trait ParentType {
  def readChild[T]: Option[T]

  def addChild(value: Any): Unit
}

