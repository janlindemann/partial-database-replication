package de.hpi.epic.partialTableDistribution.cluster.base

import util.control.Breaks._

/**
  * Created by Jan on 09.07.2017.
  */
abstract class Worker[T](generator: Generator[T]) extends Runnable {
  def doWork(value: T): Unit

  override def run(): Unit = {
    breakable {
      while (true) {
        generator.next match {
          case Some(value) => doWork(value)
          case None => break
        }
      }
    }
  }
}
