package de.hpi.epic.partialTableDistribution.algorithms.adaptive

import de.hpi.epic.partialTableDistribution.data.{Fragment, Side}

import scala.collection.{Map, Seq}

/**
  * Created by Jan on 22.11.2017.
  */
trait CostModel {
  def transmissionCosts(previousDist: Map[Side, Seq[Fragment]], newDist: Map[Side, Seq[Fragment]]): TransmissionCost
  def byteDiff(previousDist: Map[Side, Seq[Fragment]], newDist: Map[Side, Seq[Fragment]]): Long = {
    newDist.map(t => {
      val (side, newFragments) = t
      val fragmentDiffs = previousDist.get(side).map(oldFragments => newFragments.diff(oldFragments)).getOrElse(newFragments)
      fragmentDiffs.view.map(_.size).sum
    }).sum
  }
}

case class TransmissionCost(realTime: Double, procTime: Double)

class LocalLogCostModel(val byteTransferRate: Long) extends CostModel {
  override def transmissionCosts(previousDist: Map[Side, Seq[Fragment]], newDist: Map[Side, Seq[Fragment]]): TransmissionCost = {
    val diffs = newDist.map(t => {
      val (side, newFragments) = t
      val fragmentDiffs = previousDist.get(side).map(oldFragments => newFragments.diff(oldFragments)).getOrElse(newFragments)
      fragmentDiffs.view.map(_.size).sum
    })
    val procTime = diffs.sum / byteTransferRate.toDouble
    val realTime = diffs.max / byteTransferRate.toDouble
    TransmissionCost(realTime, procTime)
  }
}

class EthernetCostModel(val byteTransferRate: Long) extends CostModel {
  override def transmissionCosts(previousDist: Map[Side, Seq[Fragment]], newDist: Map[Side, Seq[Fragment]]): TransmissionCost = {
    val diffs = newDist.map(t => {
      val (side, newFragments) = t
      val fragmentDiffs = previousDist.get(side).map(oldFragments => newFragments.diff(oldFragments)).getOrElse(newFragments)
      fragmentDiffs.view.map(_.size).sum
    })
    val procTime = diffs.sum / byteTransferRate.toDouble
    TransmissionCost(procTime, procTime)
  }
}