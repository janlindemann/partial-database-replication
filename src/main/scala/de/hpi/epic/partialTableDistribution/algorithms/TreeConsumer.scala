package de.hpi.epic.partialTableDistribution.algorithms

import java.util.concurrent.BlockingQueue

import de.hpi.epic.partialTableDistribution.utils.{Column, Query}

/**
  * Created by Jan on 21.06.2017.
  */
class TreeConsumer(queue: BlockingQueue[Seq[Map[Int, Seq[Query]]]], resultCombiner: ResultCombiner[Map[Int, Seq[Query]]]) extends Consumer[Seq[Map[Int, Seq[Query]]]](queue) {
  var minValue: Long = Long.MaxValue
  var minDistr: Map[Int, Seq[Query]] = Map.empty[Int, Seq[Query]]

  override def consume(x: Seq[Map[Int, Seq[Query]]]): Boolean = {
    if (x.isEmpty) {
      if (minDistr.nonEmpty) resultCombiner.append(minDistr)
      false
    }
    else {
      x.foreach(d => {
        val bucketSize = d.map {
          case (_, bucket) => bucket.foldLeft(Set.empty[Column])((p,c) => p ++ c.columns).foldLeft(0)((p,c) => p + c.size)
        }.sum
        if (minValue > bucketSize) {
          minValue = bucketSize
          minDistr = d
        }
      })
      true
    }
  }
}
