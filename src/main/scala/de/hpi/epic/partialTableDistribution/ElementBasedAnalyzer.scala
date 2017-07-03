package de.hpi.epic.partialTableDistribution
import de.hpi.epic.partialTableDistribution.algorithms.SingleBucket
import de.hpi.epic.partialTableDistribution.utils.Query

/**
  * Created by Jan on 19.06.2017.
  */
class ElementBasedAnalyzer(val tablesFileName: String, val queriesFileName: String) extends NewAnalyzer[Int] {
  override protected def buildBuckets(input: Int): Seq[(Seq[Query], Long)] = SingleBucket.distributeBySize(queries, input)
  override protected val range: Seq[Int] = Range.inclusive(1, queries.length)
}

object ElementBasedAnalyzer {
  def main(args: Array[String]): Unit = {
    val analyzer = new ElementBasedAnalyzer("./attribute_sizes_new.txt", "./tpc-h_queries_part.txt")
    analyzer.evaluate
  }
}