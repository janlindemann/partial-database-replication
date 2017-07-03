package de.hpi.epic.partialTableDistribution.algorithms

import java.util.concurrent.BlockingQueue

import scala.util.control.Breaks.{break, breakable}

/**
  * Created by Jan on 21.06.2017.
  */
abstract class Consumer[T](queue: BlockingQueue[T]) extends Runnable {
  def run() {
    breakable {
      while (true) {
        val item = queue.take()
        if (!consume(item)) {
          queue.put(item)
          break()
        }
      }
    }
  }

  def consume(x: T): Boolean
}
