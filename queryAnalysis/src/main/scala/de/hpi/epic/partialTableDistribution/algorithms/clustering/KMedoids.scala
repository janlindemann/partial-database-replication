package de.hpi.epic.partialTableDistribution.algorithms.clustering

import scala.util.Random

/**
  * Created by Jan on 17.07.2017.
  */
class KMedoids[T](k: Int, metric: (T, T) => Double) {
  def train(data: Seq[T]): Seq[T] = {
    var medoids = initialize(data)
    var modelConverged = false
    var metric = Double.MaxValue
    while (!modelConverged) {
      val clusters = data.groupBy(medoidIdx(_, medoids)).values
      val nextMetric = clusters.zip(medoids).foldLeft(0d)((p, c) => p + medoidCost(c._2, c._1))
      if (metric == nextMetric) {
        modelConverged = true
      } else {
        metric = nextMetric
        medoids = clusters.map(medoid).toSeq
      }
    }
    medoids
  }

  def cluster(data: Seq[T], medoids: Seq[T]): Seq[Seq[T]] = data.groupBy(medoidIdx(_, medoids)).values.toSeq

  private def medoid(data: Seq[T]) = data.minBy(medoidCost(_, data))

  private def medoidCost(e: T, data: Seq[T]) = data.iterator.map(metric(e, _)).sum

  private def medoidIdx(e: T, mv: Seq[T]) = mv.iterator.map(metric(e, _)).zipWithIndex.min._2

  private def initialize(data: Seq[T]): Seq[T] = {
    assert(k >= 1)
    assert(data.length >= k)

    val ids = for {_ <- 0 until k} yield Random.nextInt(data.length)
    data.zipWithIndex.filter(e => ids.contains(e._2)).map(_._1)
  }
}
