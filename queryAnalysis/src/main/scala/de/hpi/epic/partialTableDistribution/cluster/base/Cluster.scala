package de.hpi.epic.partialTableDistribution.cluster.base


/**
  * Created by Jan on 09.07.2017.
  */
abstract class Cluster[T](generator: Generator[T]) {
  protected def execute(buildWorker: (Int) => Runnable) = {
    val t = for {
      i <- 0 until Runtime.getRuntime.availableProcessors()
    } yield new Thread(buildWorker(i))

    t.foreach(_.start())
    t.foreach(_.join())
  }
}