package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.data.{ReadQuery, UpdateQuery}
import de.hpi.epic.partialTableDistribution.data.{Column, Table, Query}
import de.hpi.epic.partialTableDistribution.parser.{TPCHParser, TableParser}
import de.hpi.epic.partialTableDistribution.sql.{Extractor, SQLElement, Select}
import de.hpi.epic.partialTableDistribution.utils.{CSVFile}

import scala.io.Source

/**
  * Created by Jan on 10.07.2017.
  */
trait Analyzer extends Workload {
  val tablesFileName: String
  val queriesFileName: String
  val sharesFileName: String

  private val pattern = "(\\d+),(\\d+)%".r

  val tables: Seq[Table] = loadTables
  val queries: Seq[Query] = loadQueries

  val sep = ";"

  lazy val fullColumnCount: Int = tables.foldLeft(0)(_ + _.columns.length)
  lazy val fullColumnSize: Long = tables.foldLeft(0l)(_ + _.columns.foldLeft(0l)(_ + _.size))

  private def loadTables = {
    val fileContents = Source.fromFile(tablesFileName).getLines().mkString(System.lineSeparator())
    TableParser.parse(fileContents)
  }

  private def fromPercentage(s: String): BigDecimal = s match {
    case pattern(l,r) => BigDecimal(s"$l.$r") / 100
  }

  private def loadQueries = {
    val shares = CSVFile(sharesFileName)
    val sqlQueries = Source.fromFile(queriesFileName).getLines().mkString(System.lineSeparator())
    TPCHParser
      .parseAll(sqlQueries)
      .map{
        case (index, s: Select) =>
          val columns = Extractor.fold(Seq.empty[Column], getColumns(tables), s)
          val share = shares.find(_.getInt("queryID") == index).map(r => fromPercentage(r.getString("load"))).getOrElse(BigDecimal(0))
          Query(index.toString, columns, share, ReadQuery)
        case (index, q) =>
          val columns = Extractor.fold(Seq.empty[Column], getColumns(tables), q)
          val share = shares.find(_.getInt("queryID") == index).map(r => fromPercentage(r.getString("load"))).getOrElse(BigDecimal(0))
          Query(index.toString, columns, share, UpdateQuery)
      }
  }

  private def getColumns(tables: Seq[Table])(p: Seq[Column], c: SQLElement): Seq[Column] = {
    c match {
      case sql.Column(name, Some(table_name)) =>
        //TODO: alias!
        tables.flatMap(_.columns.find(_.name.toLowerCase == name.toLowerCase)).headOption.map(_ +: p).getOrElse(p)
      case sql.Column(name, None) =>
        tables.flatMap(_.columns.find(_.name.toLowerCase == name.toLowerCase)).headOption.map(_ +: p).getOrElse(p)
      case _ => p
    }
  }
}
