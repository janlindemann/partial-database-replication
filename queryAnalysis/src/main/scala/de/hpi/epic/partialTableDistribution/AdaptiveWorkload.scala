package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.adaptive.{AdaptiveHeuristic, CostModel, LocalLogCostModel, TransmissionCost}
import de.hpi.epic.partialTableDistribution.algorithms.heuristics.{Allocation, Heuristic, QueryCentricPartitioningHeuristic}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import de.hpi.epic.partialTableDistribution.data._
import de.hpi.epic.partialTableDistribution.parser.visitor.{StatementVisitor, TableMetadataExtractor}
import de.hpi.epic.partialTableDistribution.utils.{CSVFile, Tabulator}

import scala.collection.Map
import scala.io.Source

/**
  * Created by Jan on 16.12.2017.
  */
trait AdaptiveWorkload {
  type Workload = Seq[Query]
  val tablesFileName: String
  val tableSizeFileName: String
  val queriesFileName: String
  val sharesFileNames: Seq[String]

  def tables: Seq[Table]
  def queries: IndexedSeq[Workload]
}
