package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.heuristics.QueryCentricPartitioningHeuristic
import de.hpi.epic.partialTableDistribution.data.{Query, ReadQuery, Table}
import de.hpi.epic.partialTableDistribution.parser.visitor.{StatementVisitor, TableMetadataExtractor}
import de.hpi.epic.partialTableDistribution.utils.CSVFile
import net.sf.jsqlparser.parser.CCJSqlParserUtil

import scala.io.Source

/**
  * Created by Jan on 17.12.2017.
  */
trait AdaptiveWorkloadLoader extends AdaptiveWorkload {
  val tables: Seq[Table] = TableMetadataExtractor.extractTables(tablesFileName, tableSizeFileName)
  val queries: IndexedSeq[Workload] = loadQueries

  private def loadQueries: IndexedSeq[Workload] = {
    //read sql trace
    val workload = Source.fromFile(queriesFileName).getLines().mkString("\n")

    //parse sql trace
    val stmt = CCJSqlParserUtil.parseStatements(workload)

    //detect referenced fragments
    val v = new StatementVisitor(tables)
    stmt.accept(v)

    sharesFileNames.map(path => {
      val shares = CSVFile(path)
      v.columnMap.map(t => {
        val share = shares.find(_.getInt("queryID") == t._1).map(_.getBigDecimal("load")).getOrElse(BigDecimal(0))
        Query(t._1.toString, t._2.toSeq, share, ReadQuery)
      }).toList.filterNot(_.load == BigDecimal(0))
    }).toIndexedSeq
  }

}

object DynamicTPCHWorkload extends AdaptiveWorkloadLoader with AdaptiveHeuristicEvaluation {
  override lazy val tablesFileName: String = "./workloads/tpch/create_tpch.sql"
  override lazy val tableSizeFileName: String = "./workloads/tpch/row_count.csv"
  override lazy val queriesFileName: String = "./workloads/tpch/workload.sql"
  override lazy val sharesFileNames: Seq[String] = Seq(
    "./workloads/tpch/complete/timings0.csv",
    "./workloads/tpch/complete/timings1.csv",
    "./workloads/tpch/complete/timings2.csv",
    "./workloads/tpch/complete/timings3.csv",
    "./workloads/tpch/complete/timings4.csv",
    "./workloads/tpch/complete/timings5.csv",
    "./workloads/tpch/complete/timings6.csv",
    "./workloads/tpch/complete/timings7.csv",
    "./workloads/tpch/complete/timings8.csv",
    "./workloads/tpch/complete/timings9.csv",
    "./workloads/tpch/complete/timings10.csv"
  )

  def main(args: Array[String]): Unit = {
    val path =  "D:/Dokumente/Masterarbeit/thesis/data/dynamic/tpch"
    evaluateStaticHeuristic(new QueryCentricPartitioningHeuristic(), Seq(5,6,6,5,4,5,5,5,5,6,5), BigDecimal("0.2"), path)
    evaluateDynamicHeuristic(5, BigDecimal("0.2"), path)
  }
}

object DynamicTPCDSWorkload extends AdaptiveWorkloadLoader with AdaptiveHeuristicEvaluation {
  override lazy val tablesFileName: String = "./workloads/tpcds/create_tpcds.sql"
  override lazy val tableSizeFileName: String = "./workloads/tpcds/row_count.csv"
  override lazy val queriesFileName: String = "./workloads/tpcds/workload.sql"
  override lazy val sharesFileNames: Seq[String] = Seq(
    "./workloads/tpcds/complete/timings0.csv",
    "./workloads/tpcds/complete/timings1.csv",
    "./workloads/tpcds/complete/timings2.csv",
    "./workloads/tpcds/complete/timings3.csv",
    "./workloads/tpcds/complete/timings4.csv",
    "./workloads/tpcds/complete/timings5.csv",
    "./workloads/tpcds/complete/timings6.csv",
    "./workloads/tpcds/complete/timings7.csv",
    "./workloads/tpcds/complete/timings8.csv",
    "./workloads/tpcds/complete/timings9.csv",
    "./workloads/tpcds/complete/timings10.csv"
  )

  def main(args: Array[String]): Unit = {
    val path =  "D:/Dokumente/Masterarbeit/thesis/data/dynamic/tpcds"
    evaluateStaticHeuristic(new QueryCentricPartitioningHeuristic(), Seq(10,11,11,10,8,10,9,9,11,11,10), BigDecimal("0.1"), path)
    evaluateDynamicHeuristic(10, BigDecimal("0.1"), path)
  }
}