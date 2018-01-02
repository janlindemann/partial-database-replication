package de.hpi.epic.partialTableDistribution.algorithms.heuristics

import de.hpi.epic.partialTableDistribution.data.{Application, Fragment, Side}

import scala.collection._

trait Heuristic {
  type Matrix[T] = Array[Array[T]]

  def distribute(applications: Seq[Application], sides: Seq[Side]): Map[Side, Seq[Fragment]]
}

trait WithContinue {
  self: Heuristic =>

  def continue(distribution: Allocation, remaining: Seq[Application]): Allocation
}

trait Allocation {
  def loadMap: Map[Side, Map[Application, BigDecimal]]
  def fragmentMap: Map[Side, Seq[Fragment]]
}

object Allocation {
  def apply(map: Map[Side, Map[Application, BigDecimal]]): Allocation = new Allocation {
    override val loadMap: Map[Side, Map[Application, BigDecimal]] = map
    override lazy val fragmentMap: Map[Side, Seq[Fragment]] = loadMap.mapValues(_.keys.toSeq.flatMap(_.fragments).distinct)
  }
  def apply(m1: Map[Side, Map[Application, BigDecimal]], m2:  Map[Side, Seq[Fragment]]): Allocation = new Allocation {
    override val loadMap: Map[Side, Map[Application, BigDecimal]] = m1
    override val fragmentMap: Map[Side, Seq[Fragment]] = m2
  }
  def transform(map: Map[(Side, Application), BigDecimal]): Allocation = new Allocation {
    override val loadMap: Map[Side, Map[Application, BigDecimal]] =
      map.foldLeft(Map.empty[Side, Map[Application, BigDecimal]])((m, t) => {
        if (t._2 == BigDecimal(0)) m
        else m.updated(t._1._1, m.getOrElse(t._1._1, Map.empty[Application, BigDecimal]) + (t._1._2 -> t._2))
      })
    override lazy val fragmentMap: Map[Side, Seq[Fragment]] = loadMap.mapValues(_.keys.toSeq.flatMap(_.fragments).distinct)
  }
}