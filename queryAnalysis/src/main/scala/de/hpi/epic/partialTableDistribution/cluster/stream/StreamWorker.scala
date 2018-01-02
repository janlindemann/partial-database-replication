package de.hpi.epic.partialTableDistribution.cluster.stream

import de.hpi.epic.partialTableDistribution.algorithms.ResultCombiner
import de.hpi.epic.partialTableDistribution.cluster.base.{Generator, Worker}

/**
  * Created by Jan on 09.07.2017.
  */
private abstract class StreamWorker[T](generator: Generator[T]) extends Runnable {
  def stream: Stream[T] = generator.next match {
    case Some(value) => value #:: stream
    case None => Stream.empty[T]
  }
}

private object StreamWorker {
  def apply[T](generator: Generator[T], f: Stream[T] => Unit) =
    new StreamWorker[T](generator) {
      override def run(): Unit = f(stream)
    }

  def apply[T, S](generator: Generator[T], f: Stream[T] => S, resultCombiner: ResultCombiner[S]) =
    new StreamWorker[T](generator) {
      override def run(): Unit = resultCombiner.append(f(stream))
    }}