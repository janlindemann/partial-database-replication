package de.hpi.epic.partialTableDistribution

import java.io.{BufferedWriter, File, FileWriter}

import de.hpi.epic.partialTableDistribution.algorithms.heuristics._
import de.hpi.epic.partialTableDistribution.data.{Application, Query, ReadQuery, Table}
import de.hpi.epic.partialTableDistribution.parser.visitor.{StatementVisitor, TableMetadataExtractor}
import de.hpi.epic.partialTableDistribution.utils.CSVFile

import scala.io.Source

/**
  * Created by Jan on 02.11.2017.
  */
trait WorkloadLoader extends Workload {
  val tablesFileName: String
  val tableSizeFileName: String
  val queriesFileName: String
  val sharesFileName: String

  val tables: Seq[Table] = TableMetadataExtractor.extractTables(tablesFileName, tableSizeFileName)
  val queries: Seq[Query] = loadQueries

  private lazy val pattern = "(\\d+),(\\d+)".r
  private def fromPercentage(s: String): BigDecimal = s match {
    case pattern(l,r) => BigDecimal(s"$l.$r") / 100
  }

  private def loadQueries = {
    val shares = CSVFile(sharesFileName)
    val workload = Source.fromFile(queriesFileName).getLines().mkString("\n")
    import net.sf.jsqlparser.parser.CCJSqlParserUtil
    val stmt = CCJSqlParserUtil.parseStatements(workload)
    val v = new StatementVisitor(tables)
    stmt.accept(v)
    v.columnMap.map(t => {
      val share = shares.find(_.getInt("queryID") == t._1).map(r => fromPercentage(r.getString("load"))).getOrElse(BigDecimal(0))
      Query(t._1.toString, t._2.toSeq, share, ReadQuery)
    }).toList
  }
}

object TPCDSWorkload extends WorkloadLoader with WorkloadAnalyzer with HeuristicEvaluation {
  override lazy val tablesFileName: String = "./workloads/tpcds/create_tpcds.sql"
  override lazy val tableSizeFileName: String = "./workloads/tpcds/row_count.csv"
  override lazy val queriesFileName: String = "./workloads/tpcds/workload.sql"
  override lazy val sharesFileName: String = "./workloads/tpcds/tpc-ds timings.csv"

  def main(args: Array[String]): Unit = {
    queries.foreach(q => println(s"${q.name}(${q.load}): ${q.fragments.map(_.name).mkString(", ")}"))
    //workloadStatistics
    //columnAccessDistribution
    //tableUsage

    def memSize(s1: Seq[Application], s2: Seq[Application]): Double = {
      val fragments1 = s1.flatMap(_.fragments).distinct
      val fragments2 = s2.flatMap(_.fragments).distinct

      val x = fragments1.diff(fragments2).foldLeft(0l)(_ + _.size)
      val y = fragments2.diff(fragments1).foldLeft(0l)(_ + _.size)
      x + y
    }
    analyzes(Seq(new MaxWeightHeuristic(memSize)), "D:/Dokumente/Masterarbeit/thesis/data/tpcds", 2, 25)
  }
}