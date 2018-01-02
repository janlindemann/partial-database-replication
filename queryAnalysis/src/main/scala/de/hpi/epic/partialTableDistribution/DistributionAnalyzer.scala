package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.{PCBinPacking, ParallelBinPacking}
import de.hpi.epic.partialTableDistribution.data.{Column, Fragment, Query}
import de.hpi.epic.partialTableDistribution.parser.{TPCHParser, TableParser}
import de.hpi.epic.partialTableDistribution.sql.{Extractor, SQLElement, Column => SQLColumn}
import de.hpi.epic.partialTableDistribution.utils._

import scala.io.Source

/**
  * Created by Jan on 07.06.2017.
  */
class DistributionAnalyzer(override val tablesFileName: String,
                           override val queriesFileName: String,
                           override val sharesFileName: String) extends Analyzer {

  def evaluate(bucketCount: Int) = {
    val min = ParallelBinPacking.execute[Query](queries, bucketCount, _.map(_._2.flatMap(_.fragments).distinct.map(_.size).sum).sum)
    val sizes = min.map(_._2.foldLeft(Set.empty[Fragment])((p, c) => p ++ c.fragments).foldLeft(0l)((p, c) => p + c.size))
    println(sizes.sum.GB)
    println(min.map(_._2.map(_.name).mkString("[", ", ", "]")).mkString("[", ",", "]"))
    println(sizes.map(_.GB).mkString("[", ", ", "]"))
  }
}

object DistributionAnalyzer {
  def main(args: Array[String]): Unit = {
    val analyzer = new DistributionAnalyzer( "./attribute_sizes_new.txt", "./tpc-h_queries_part.txt", "../tpc-h/tpc-h timings monet.csv")
    analyzer.evaluate(2)
  }
}
