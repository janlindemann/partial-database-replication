package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.{BinPacking, PCBinPacking}
import de.hpi.epic.partialTableDistribution.parser.{TPCHParser, TableParser}
import de.hpi.epic.partialTableDistribution.sql.{Column => SQLColumn, Extractor, SQLElement}
import de.hpi.epic.partialTableDistribution.utils._

import scala.io.Source

/**
  * Created by Jan on 07.06.2017.
  */
object DistributionAnalyzer {
  def getColumns(tables: Seq[utils.Table])(p: Seq[utils.Column], c: SQLElement): Seq[utils.Column] = {
    c match {
      case SQLColumn(name, Some(table_name)) =>
      tables.find(_.name.toLowerCase == table_name.toLowerCase).flatMap(_.columns.find(_.name.toLowerCase == name.toLowerCase)).map(_ +: p).getOrElse(p)
      case SQLColumn(name, None) =>
      tables.flatMap(_.columns.find(_.name.toLowerCase == name.toLowerCase)).headOption.map(_ +: p).getOrElse(p)
      case _ => p
    }
  }

  def main(args: Array[String]): Unit = {
    val fileContents = Source.fromFile("./attribute_sizes_new.txt").getLines().mkString(sys.props("line.separator"))
    val tables = TableParser.parse(fileContents)

    val sqlQueries = Source.fromFile("./tpc-h_queries_part.txt").getLines().mkString(sys.props("line.separator"))
    val queries = TPCHParser
      .parseAll(sqlQueries)
      .map(q => utils.Query(q._1.toString, Extractor.fold(Seq.empty[utils.Column], getColumns(tables), q._2).toSet))

    /*val res = BinPacking.apply(queries, 2)
    println(s"${res.length} possibilities")
    val sorted = res.sortBy(_._3)

    val min = sorted.head
    import de.hpi.epic.partialTableDistribution.utils._
    println(
      min._3.GB + ": " +
        min._1.values.map(_.map(_.id).mkString("[", ", ", "]")).mkString("[", ", ", "]") + " " +
        min._2.map(_.GB).mkString("[", ", ", "]"))

    val max = sorted.last
    println(
      max._3.GB + ": " +
        max._1.values.map(_.map(_.id).mkString("[", ", ", "]")).mkString("[", ", ", "]") + " " +
        max._2.map(_.GB).mkString("[", ", ", "]"))*/

    val res = PCBinPacking.execute(queries, 2)
    val sizes = res.map(_._2.foldLeft(Set.empty[utils.Column])((p,c) => p ++ c.columns).foldLeft(0)((p,c) => p + c.size))
    println(sizes.sum.GB)
    println(res.map(_._2.map(_.id).mkString("[", ", ", "]")).mkString("[", ",", "]"))
    println(sizes.map(_.GB).mkString("[", ", ", "]"))
  }
}
