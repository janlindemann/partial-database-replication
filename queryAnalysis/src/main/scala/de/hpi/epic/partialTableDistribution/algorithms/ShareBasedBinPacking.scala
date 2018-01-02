package de.hpi.epic.partialTableDistribution.algorithms

import de.hpi.epic.partialTableDistribution.cluster.base.Generator
import de.hpi.epic.partialTableDistribution.cluster.stream.StreamCluster
import de.hpi.epic.partialTableDistribution.utils.XMath
import de.hpi.epic.partialTableDistribution.utils.XMath.faculty

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.JavaConversions
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by Jan on 09.07.2017.
  */
class ShareBasedBinPacking[T](elements: Seq[T], shares: Map[T, Double]) {
  private val sortedElements = elements.sortBy( -1 * shares.getOrElse(_, 0d) )

  @tailrec
  private def calculateMaxDistributionCount(remainingQueryCount: Int, bucketCount: Int, openBuckets: Int, result: Long = 1l): Long = {
    if (remainingQueryCount == 0) result
    else if (openBuckets < bucketCount) calculateMaxDistributionCount(remainingQueryCount - 1, bucketCount, openBuckets + 1, result * (openBuckets + 1))
    else calculateMaxDistributionCount(remainingQueryCount - 1, bucketCount, openBuckets, result * bucketCount)
  }

  def distribute(bucketCount: Int, difference: Double): Seq[Map[Int, Seq[T]]] = {
    val distribution = Map.empty[Int, (Double, Seq[T])]
    recursiveBucketBuilder(distribution, sortedElements, 1 / bucketCount.toDouble + difference, bucketCount)
  }

  def distributePart(bucketCount: Int, difference: Double, range: (Long, Long)): Seq[Map[Int, Seq[T]]] = {
    val distribution = Map.empty[Int, (Double, Seq[T])]
    recursiveBucketBuilderPart(distribution, sortedElements, 1 / bucketCount.toDouble + difference, bucketCount, range._1, 0l, range._2)
  }

  private def recursiveBucketBuilderPart(distribution: Map[Int, (Double, Seq[T])], remainingElements: Seq[T],
                                         maxBucketSize: Double, bucketCount: Int, offset: Long, curOffset: Long,
                                         endOffset: Long): Seq[Map[Int, Seq[T]]] =
    remainingElements match {
      case head :: tail =>
        val it = distribution.iterator
        val result = new scala.collection.mutable.ListBuffer[Map[Int, Seq[T]]]()
        var nextOffset = curOffset
        breakable {
          while (it.hasNext) {
            if (nextOffset >= endOffset) break()
            val (key, (share, seq)) = it.next()
            val possibilities = calculatePossibilitiesPerf(math.min(distribution.size + 1, bucketCount), tail.size, bucketCount)
            if (offset < nextOffset + possibilities) {
              val updatedShare = share + shares(head)
              if (updatedShare < maxBucketSize) {
                val newDistribution = distribution.updated(key, (updatedShare, head +: seq))
                result ++= recursiveBucketBuilderPart(newDistribution, tail, maxBucketSize, bucketCount, offset, nextOffset, endOffset)
              }
              nextOffset += possibilities
            } else {
              nextOffset += possibilities
            }
          }
        }
        val count = distribution.size
        if (count < bucketCount && nextOffset < endOffset) {
          val newDistribution = distribution + (count + 1 ->(shares(head), Seq(head)))
          result ++= recursiveBucketBuilderPart(newDistribution, tail, maxBucketSize, bucketCount, offset, nextOffset, endOffset)
        }
        result
      case _ => Seq(distribution.map(t => (t._1, t._2._2)))
    }

  private def recursiveBucketBuilder(distribution: Map[Int, (Double, Seq[T])],
                                        remainingQueries: Seq[T],
                                        maxBucketSize: Double,
                                        bucketCount: Int): Seq[Map[Int, Seq[T]]] = {
    remainingQueries match {
      case head :: tail =>
        val it = distribution.iterator
        val result = new scala.collection.mutable.ListBuffer[Map[Int, Seq[T]]]()
        while (it.hasNext) {
          val (key, (share, seq)) = it.next()
          val updatedShare = share + shares(head)
          if (updatedShare < maxBucketSize) {
            val newDistribution = distribution.updated(key, (updatedShare, head +: seq))
            result ++= recursiveBucketBuilder(newDistribution, tail, maxBucketSize, bucketCount)
          }
        }
        val count = distribution.size
        if (count < bucketCount ) {
          val newDistribution = distribution + (count + 1 ->(shares(head), Seq(head)))
          result ++= recursiveBucketBuilder(newDistribution, tail, maxBucketSize, bucketCount)
        }
        result
      case _ => Seq(distribution.map(t => (t._1, t._2._2)))
    }
  }
}

object calculatePossibilitiesPerf {
  private val map = collection.concurrent.TrieMap.empty[(Long, Long, Long), Long]
  private def calculate(openBuckets: Long, level: Long, bucketCount: Long): Long =
    if (openBuckets >= bucketCount) {
      XMath.pow(openBuckets, level)
    } else {
      import XMath._
      ((openBuckets - 1) ^^ (level - 1)) * openBuckets + SumRange(1, level-1, i => ((openBuckets -1) ^^ (level - i - 1)) * calculatePossibilitiesPerf(openBuckets + 1, i, bucketCount))
    }

  def apply(openBuckets: Long, level: Long, bucketCount: Long): Long = {
    map.getOrElseUpdate((openBuckets, level, bucketCount), calculate(openBuckets, level, bucketCount))
  }
}

object ShareBasedBinPacking {
  class RangeGenerator(range: Long, end: Long) extends Generator[(Long, Long)] {
    var pos = 0l
    def next: Option[(Long, Long)] = {
      var res = 0l
      this.synchronized({
        res = pos
        pos += range
      })
      if (res % 10000000L == 0) println(res)
      if (res <= end) Some((res, Math.min(res + range, end))) else None
    }
  }

  def main(args: Array[String]): Unit = {
    val s = Seq(1,2,3,4)
    val shares = Map(1 -> 0.4, 2 -> 0.1, 3 -> 0.1, 4 -> 0.4)
    val analyzer = new ShareBasedBinPacking(s, shares)

    val bucketCount = 2
    val count = calculatePossibilitiesPerf(1, s.size, bucketCount)
    val generator = new RangeGenerator(2, count)
    val res = StreamCluster(generator).map(t =>
      analyzer.distributePart(bucketCount, 0.05, t)
    ).flatten.minBy(_.map(_._2.foldRight(0)(_-_)).sum)
    println(res.map(_._2.mkString("[",",","]")).mkString("[",",","]"))
  }
}