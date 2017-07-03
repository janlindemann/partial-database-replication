package de.hpi.epic.partialTableDistribution
import de.hpi.epic.partialTableDistribution.algorithms.SingleBucket
import de.hpi.epic.partialTableDistribution.utils.{CSVLoader, Query}

import scala.io.Source

/**
  * Created by Jan on 19.06.2017.
  */
class ShareBasedAnalyzer(val tablesFileName: String, val queriesFileName: String, val sharesFileName: String) extends NewAnalyzer[Double] {
  private val pattern = "(\\d+),(\\d+)%".r
  private def fromPercentage(s: String) = s match {
    case pattern(l,r) => (l.toInt + r.toInt / 100.0d) / 100.0d
  }

  private def loadShares = CSVLoader.load(sharesFileName).tail.flatMap(s => queries.find(_.id == s.head).map((_, fromPercentage(s.last)))).toMap

  val shares = loadShares
  override protected def buildBuckets(input: Double): Seq[(Seq[Query], Long)] = SingleBucket.distributeByShare(queries, shares, input)
  override protected val range: Seq[Double] = Range.Double.inclusive(0.1,1,0.1)
}

object ShareBasedAnalyzer {
  def main(args: Array[String]): Unit = {
    val analyzer = new ShareBasedAnalyzer("./attribute_sizes_new.txt", "./tpc-h_queries_part.txt", "../tpc-h/tpc-h timings monet.csv")
    analyzer.evaluate
  }
}
