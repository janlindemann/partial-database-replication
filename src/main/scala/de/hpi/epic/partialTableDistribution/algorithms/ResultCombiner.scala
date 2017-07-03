package de.hpi.epic.partialTableDistribution.algorithms

import scala.collection.mutable

/**
  * Created by Jan on 21.06.2017.
  */
class ResultCombiner[T] {
  val res = new mutable.Queue[T]()
  def append(e: T): Unit = synchronized {
    res += e
  }
}
