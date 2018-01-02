package de.hpi.epic.partialTableDistribution.algorithms

import de.hpi.epic.partialTableDistribution.cluster.base.Generator
import de.hpi.epic.partialTableDistribution.cluster.stream.StreamCluster

import scala.annotation.tailrec
import util.control.Breaks._

/**
  * Created by Jan on 19.06.2017.
  */
object ParallelBinPacking {
  class RangeGenerator(range: Long, end: Long) extends Generator[(Long, Long)] {
    var pos = 0l
    def next: Option[(Long, Long)] = {
      var res = 0l
      this.synchronized({
        res = pos
        pos += range
      })
      if (res % 100000 == 0) println(res)
      if (res <= end) Some((res, Math.min(res + range, end))) else None
    }
  }

  @tailrec
  def faculty(l: Long, s : Long = 1l): Long = {
    if (l <= 0) s else faculty(l - 1, l * s)
  }

  @tailrec
  //(N choose K+1) = (N choose K) * (N-K)/(K+1)
  def choose(n: Long, k: Long, r: Long = 1l, res: Long = 1l): Long = {
    if (r > k) res else choose(n, k, r + 1, res * (n + 1 - r) / r)
  }

  def calculatePossibilities[T](distribution: Map[Long, Seq[T]], bucketSize: Long, remaining: Seq[T]): Long = {
    val l = distribution.size
    val missingElements = distribution.foldLeft(0l)((l,r) => l + (bucketSize - r._2.length))
    val count = distribution
      .filter(_._2.length < bucketSize)
      .foldLeft((1l, missingElements))((p, c) => {
        val nCr = choose(p._2, bucketSize - c._2.length)
        if (c._2.isEmpty) (p._1 * (nCr / (l - c._1)), p._2 - bucketSize)
        else (p._1 * nCr, p._2 - (bucketSize - c._2.length))
      })._1

    val remainingElements = remaining.length - missingElements
    val toMuchElements = distribution.filter(_._2.length > bucketSize).size
    count * (faculty(distribution.size - toMuchElements) / faculty(distribution.size - remainingElements - toMuchElements))
  }

  def recursiveBucketBuilderPart2[T](distribution: Map[Long, Seq[T]], remainingQueries: Seq[T], bucketSize: Long,
                                     offset: Long, curOffset: Long, amount: Long): Seq[Map[Long, Seq[T]]] = {
    remainingQueries match {
      case head :: tail =>
        val buckets = distribution.filter(_._2.size == bucketSize)
        val it = buckets.iterator
        val result = new scala.collection.mutable.ListBuffer[Map[Long, Seq[T]]]()
        breakable {
          var nextOffset = curOffset
          while (it.hasNext) {
            if (result.size >= amount) break()
            val (key, value) = it.next()
            val newDistribution = distribution.updated(key, head +: value)
            val possibilities = calculatePossibilities(newDistribution, bucketSize, tail)
            if (offset < nextOffset + possibilities) {
              result ++= recursiveBucketBuilderPart2(newDistribution, tail, bucketSize, offset, nextOffset, amount - result.size)
              nextOffset += possibilities
            } else {
              nextOffset += possibilities
            }
          }
        }
        result.toSeq
      case _ => Seq(distribution)
    }
  }

  def recursiveBucketBuilder[T](distribution: Map[Long, Seq[T]], remainingQueries: Seq[T], bucketSize: Long,
                                      offset: Long, curOffset: Long, amount: Long): Seq[Map[Long, Seq[T]]] = {
    if (!distribution.exists(_._2.size < bucketSize))
      recursiveBucketBuilderPart2(distribution, remainingQueries, bucketSize, offset, curOffset, amount)
    else {
      val (empty, filled) = distribution.partition(_._2.isEmpty)
      remainingQueries match {
        case head :: tail =>
          val buckets =
            if (empty.nonEmpty) filled.toSeq.filter(_._2.length < bucketSize) :+ empty.minBy(_._1)
            else filled.toSeq.filter(_._2.length < bucketSize)
          val it = buckets.iterator
          val result = new scala.collection.mutable.ListBuffer[Map[Long, Seq[T]]]()
          breakable {
            var nextOffset = curOffset
            while (it.hasNext) {
              if (result.size >= amount) break()
              val (key, value) = it.next()
              val newDistribution = distribution.updated(key, head +: value)
              val possibilities = calculatePossibilities(newDistribution, bucketSize, tail)
              if (offset < nextOffset + possibilities) {
                result ++= recursiveBucketBuilder(newDistribution, tail, bucketSize, offset, nextOffset, amount - result.size)
                nextOffset += possibilities
              } else {
                nextOffset += possibilities
              }
            }
          }
          result.toSeq
        case _ => Seq(distribution)
      }
    }
  }

  def execute[T](seq: Seq[T], bucketCount: Int, min: Map[Long, Seq[T]] => Long): Map[Long, Seq[T]] = {
    val map: Map[Long, Seq[T]] = Range(0,bucketCount).map(i => (i.toLong, Seq.empty[T])).toMap
    val bucketSize = seq.length / bucketCount
    val count = calculatePossibilities(map, bucketSize, seq)
    println(count)

    val generator = new RangeGenerator(100, count)
    val result = StreamCluster(generator).map(r => {
      recursiveBucketBuilder(
        map,
        seq,
        bucketSize,
        r._1,
        0,
        r._2 - r._1
      )
    }).flatten.minBy(min)
    result
  }

  def main(args: Array[String]): Unit = {
    val bucketCount = 4
    val seq = Range(0,22)
    val map: Map[Long, Seq[Int]] = Range(0,bucketCount).map(i => (i.toLong, Seq.empty[Int])).toMap
    val bucketSize = seq.length / bucketCount
    val count = calculatePossibilities(map, bucketSize, seq)
    println(count)

    /*val generator = new RangeGenerator(2, count)
    val result = Cluster(generator).map(r => {
      recursiveBucketBuilder(
        map,
        seq,
        bucketSize,
        r._1,
        0,
        r._2 - r._1
      )
    }).flatten.toSeq
    println(result)*/
  }
}
