package de.hpi.epic.partialTableDistribution.cluster.base

/**
  * Created by Jan on 09.07.2017.
  */
trait Generator[T] {
  def next: Option[T]
}
