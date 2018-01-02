package de.hpi.epic.partialTableDistribution.algorithms

import de.hpi.epic.partialTableDistribution.data.{Column, Fragment, Query}

import scala.annotation.tailrec

/**
  * Created by Jan on 01.06.2017.
  */
object BinPacking {
  type Bucket[T] = Seq[T]
  type Distribution[T] = Seq[Bucket[T]]

  def apply(queries: Seq[Query], buckets: Int) = {
    val minElements = Math.ceil(queries.length / buckets.toFloat).toInt
    val emptyMap = (for {
      i <- 0 until buckets
    } yield (i, Seq.empty[Query])).toMap

    val possibilities = recursiveBucketBuilderPhase1(emptyMap, queries, minElements)
    possibilities.map(distribution => {
      val bucketSize = distribution.map {
        case (_, bucket) => bucket.foldLeft(Set.empty[Fragment])((p, c) => p ++ c.fragments).foldLeft(0l)((p, c) => p + c.size)
      }
      (distribution, bucketSize, bucketSize.sum)
    })
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
          buckets.flatMap{
            case (key, value) => {
              val newDistribution = distribution.updated(key, head +: value)
              recursiveBucketBuilderPhase1(newDistribution, tail, depth)
            }
          }
        case _ => Seq(distribution)
      }
    }
  }

  //used for acceleration when no empty buckets are left
  private def recursiveBucketBuilderPhase2(distribution: Map[Int, Seq[Query]], remainingQueries: Seq[Query], depth: Int): Seq[Map[Int, Seq[Query]]] = {
    remainingQueries match {
      case head :: tail =>
        distribution.withFilter(_._2.length < depth).flatMap {
          case (key, value) => {
            val newDistribution = distribution.updated(key, head +: value)
            recursiveBucketBuilderPhase2(newDistribution, tail, depth)
          }
        }.toSeq
      case _ => Seq(distribution)
    }
  }

}
