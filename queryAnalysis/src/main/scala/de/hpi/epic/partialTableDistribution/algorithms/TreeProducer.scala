package de.hpi.epic.partialTableDistribution.algorithms

import java.util.concurrent.BlockingQueue

import de.hpi.epic.partialTableDistribution.data.Query

/**
  * Created by Jan on 21.06.2017.
  */
class TreeProducer(queue: BlockingQueue[Seq[Map[Int, Seq[Query]]]], queries: Seq[Query], buckets: Int) extends Producer[Seq[Map[Int, Seq[Query]]]](queue) {
  override def run(): Unit = {
    val minElements = Math.ceil(queries.length / buckets.toFloat).toInt
    val emptyMap = (for {
      i <- 0 until buckets
    } yield (i, Seq.empty[Query])).toMap

    val possibilities = recursiveBucketBuilderPhase1(emptyMap, queries, minElements)
    publish(possibilities)

    //publish end
    publish(Seq.empty[Map[Int, Seq[Query]]])
  }

  //add only one new possibility with an empty bucket
  private def recursiveBucketBuilderPhase1(distribution: Map[Int, Seq[Query]], remainingQueries: Seq[Query], depth: Int): Seq[Map[Int, Seq[Query]]] = {
    val (empty, filled) = distribution.partition(_._2.isEmpty)
    if (empty.isEmpty) {
      recursiveBucketBuilderPhase2(distribution, remainingQueries, depth)
    }
    else {
      remainingQueries match {
        case head :: tail =>
          val buckets = empty.head +: filled.toSeq.filter(_._2.length < depth)
          val res = buckets.flatMap{
            case (key, value) => {
              val newDistribution = distribution.updated(key, head +: value)
              recursiveBucketBuilderPhase1(newDistribution, tail, depth)
            }
          }
          if (res.size >= 10) {
            publish(res.take(10))
            res.drop(10)
          } else {
            res
          }
        case _ => Seq(distribution)
      }
    }
  }

  //used for acceleration when no empty buckets are left
  private def recursiveBucketBuilderPhase2(distribution: Map[Int, Seq[Query]], remainingQueries: Seq[Query], depth: Int): Seq[Map[Int, Seq[Query]]] = {
    remainingQueries match {
      case head :: tail =>
        val res = distribution.withFilter(_._2.length < depth).flatMap {
          case (key, value) => {
            val newDistribution = distribution.updated(key, head +: value)
            recursiveBucketBuilderPhase2(newDistribution, tail, depth)
          }
        }.toSeq
        if (res.size >= 10) {
          publish(res.take(10))
          res.drop(10)
        } else {
          res
        }
      case _ => Seq(distribution)
    }
  }
}
