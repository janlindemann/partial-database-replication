package de.hpi.epic.partialTableDistribution.algorithms

import java.util.concurrent.BlockingQueue

/**
  * Created by Jan on 21.06.2017.
  */
abstract class Producer[T](queue: BlockingQueue[T]) extends Runnable {
  def publish(value: T): Unit = queue.put(value)
}
