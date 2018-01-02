package de.hpi.epic.partialTableDistribution.algorithms

import scala.util.control.Breaks._
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import de.hpi.epic.partialTableDistribution.algorithms.BinPacking.{recursiveBucketBuilderPhase1, recursiveBucketBuilderPhase2}
import de.hpi.epic.partialTableDistribution.data.{Column, Fragment, Query}

/**
  * Created by Jan on 21.06.2017.
  */
object PCBinPacking {

  def execute(queries: Seq[Query], buckets: Int): Map[Int, Seq[Query]] = {
    val cores = Runtime.getRuntime.availableProcessors()
    val queue = new ArrayBlockingQueue[Seq[Map[Int, Seq[Query]]]]((cores - 1) * 5)

    val producer = new TreeProducer(queue, queries, buckets)
    val producerThread = new Thread(producer)
    producerThread.start()

    val resultCombiner = new ResultCombiner[Map[Int, Seq[Query]]]()

    val consumer = for {
      i <- 1 until cores - 1
    } yield new Thread(new TreeConsumer(queue, resultCombiner))
    consumer.foreach(_.start())

    producerThread.join()
    consumer.foreach(_.join())

    resultCombiner.res.minBy(_.map {
        case (_, bucket) => bucket.foldLeft(Set.empty[Fragment])((p, c) => p ++ c.fragments).foldLeft(0l)((p, c) => p + c.size)
      }.sum
    )
  }

}
