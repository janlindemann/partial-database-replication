package de.hpi.epic.partialTableDistribution.cluster.stream

import de.hpi.epic.partialTableDistribution.algorithms.ResultCombiner
import de.hpi.epic.partialTableDistribution.cluster.base.{Cluster, Generator, Worker}

import scala.collection.GenTraversableOnce

/**
  * Created by Jan on 09.07.2017.
  */
class StreamCluster[T, R](generator: Generator[T], fun: Stream[T] => Stream[R]) extends Cluster[T](generator) {
  def map[S](f: R => S) =
    new StreamCluster[T, S](generator, (s: Stream[T]) => fun(s).map(f))

  def flatten[B](implicit asTraversable: R => /*<:<!!!*/ GenTraversableOnce[B]) =
    new StreamCluster(generator, (s: Stream[T]) => fun(s).flatten(asTraversable))

  def minBy[B](f: R => B)(implicit ord: Ordering[B]): R = {
    val resultCombiner = new ResultCombiner[R]
    execute(_ => StreamWorker.apply(generator, (s: Stream[T]) => fun(s).minBy(f)(ord), resultCombiner))
    resultCombiner.res.minBy(f)(ord)
  }
}

object StreamCluster {
  def apply[T](generator: Generator[T]) = new StreamCluster[T, T](generator, identity)
}