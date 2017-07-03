package de.hpi.epic.partialTableDistribution

import java.io.{BufferedWriter, File, FileWriter}
import de.hpi.epic.partialTableDistribution.algorithms.SingleBucket
import de.hpi.epic.partialTableDistribution.parser.{TPCHParser, TableParser}
import de.hpi.epic.partialTableDistribution.sql.{Extractor, SQLElement}
import de.hpi.epic.partialTableDistribution.utils.{Column, Query, Table}
import de.hpi.epic.partialTableDistribution.utils._

import scala.io.Source

/**
  * Created by Jan on 16.06.2017.
  */
trait NewAnalyzer[T] {
  val tablesFileName: String //"./attribute_sizes_new.txt"
  val queriesFileName: String
  protected def buildBuckets(input: T): Seq[(Seq[Query], Long)]
  protected val range: Seq[T]

  //general input data
  val tables: Seq[Table] = loadTables
  val queries: Seq[Query] = loadQueries

  val sep = ";"

  lazy val fullColumnCount: Int = tables.foldLeft(0)(_ + _.columns.length)
  lazy val fullColumnSize: Long = tables.foldLeft(0l)(_ + _.columns.foldLeft(0l)(_ + _.size))

  private def loadTables = {
    val fileContents = Source.fromFile(tablesFileName).getLines().mkString(sys.props("line.separator"))
    TableParser.parse(fileContents)
  }

  private def loadQueries = {
    val sqlQueries = Source.fromFile("./tpc-h_queries_part.txt").getLines().mkString(sys.props("line.separator"))
    TPCHParser
      .parseAll(sqlQueries)
      .map(q => utils.Query(q._1.toString, Extractor.fold(Seq.empty[Column], getColumns(tables), q._2).toSet))
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

  private def columnPercentage(result: (Seq[utils.Query], Long), columnCount: Int): String = {
    val columns = result._1.flatMap(_.columns).distinct
    "%.2f".format(columns.length / columnCount.toDouble * 100)
  }

  private def unaccessedColumns(queries: Seq[utils.Query], tables: Seq[Table]): Set[Column] = {
    val accessedColumns = queries.foldLeft(Set.empty[utils.Column])((p, c) => p.union(c.columns))
    tables.foldLeft(Set.empty[Column])((p,c) => p.union(c.columns.toSet)).diff(accessedColumns)
  }

  def evaluate(input: T): String = {
    val sorted = buildBuckets(input).sortBy(_._2)
    val min = sorted.head
    val minQueries = min._1.map(_.id).sortBy(_.toInt).mkString(",")
    val minCol = columnPercentage(min, fullColumnCount)
    val max = sorted.last
    val maxQueries = max._1.map(_.id).sortBy(_.toInt).mkString(",")
    val maxCol = columnPercentage(max, fullColumnCount)
    val avg = (sorted.map(_._2).sum / sorted.length).GB
    s"${input.toString}$sep$minCol$sep${(min._2 / fullColumnSize.toDouble).percentage} (${min._2.GB})$sep$minQueries$sep$maxCol$sep${(max._2 / fullColumnSize.toDouble).percentage} (${max._2.GB})$sep$maxQueries$sep$avg${sys.props("line.separator")}"
  }

  def evaluate: Unit = {
    val file = new File("./columnResults3.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    val sep = ';'
    bw.write(s"#queries$sep% columns min${sep}size min${sep}column ids$sep% columns max${sep}size max${sep}column ids${sep}size avg${sys.props("line.separator")}")

    range.par.map(i => {
      evaluate(i)
    }).seq.foreach(bw.write)
    bw.close()
  }

}
