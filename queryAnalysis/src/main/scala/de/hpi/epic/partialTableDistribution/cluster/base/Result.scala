package de.hpi.epic.partialTableDistribution.cluster.base

import de.hpi.epic.partialTableDistribution.algorithms.ResultCombiner

/**
  * Created by Jan on 09.07.2017.
  */
trait Result[T, R] { self: Cluster[T] =>
  val res = new ResultCombiner[R]
}
