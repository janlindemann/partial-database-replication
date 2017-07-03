package de.hpi.epic.partialTableDistribution.utils

/**
  * Created by Jan on 24.05.2017.
  */

trait SQLType
case object SQLInteger extends SQLType
case object SQLVarChar extends SQLType

case class Column(name: String, `type`: SQLType, length: Option[Int], rows: Int = 0) {
  lazy val size: Int = `type` match {
    case SQLInteger => 4 * rows
    case SQLVarChar => length.getOrElse(0) * rows
  }
}
