package de.hpi.epic.partialTableDistribution.algorithms.heuristics
import de.hpi.epic.partialTableDistribution.data.{Application, Fragment, Side}

import scala.collection._

/**
  * Created by Jan on 14.10.2017.
  */
class PilotMethodHeuristic extends Heuristic {

  private def updates(application: Application, updateApplications: Seq[Application]) =
    updateApplications.filter(uApp => uApp.fragments.intersect(application.fragments).nonEmpty)


  private def recDistribute(remaining: Seq[Application],
                            updateApplications: Seq[Application],
                            sides: Seq[Side],
                            currentLoads: Map[Side, BigDecimal],
                            scaledLoads: Map[Side, BigDecimal],
                            restWeight: Map[Application, BigDecimal],
                            fragmentAllocation: Map[Side, Seq[Fragment]]): Map[Side, Seq[Fragment]] = {
    val parSides = sides.par
    if (!parSides.exists(s => s.capacity != currentLoads(s))) return fragmentAllocation
    val sorted = remaining.sortBy(app => {
      if (app.load == restWeight(app)) {
        val appGroup = app +: updates(app, updateApplications)
        appGroup.foldLeft(BigDecimal(0))(_ + _.load) * appGroup.flatMap(_.fragments).distinct.map(_.size).sum * -1
      } else {
        restWeight(app) * app.fragments.map(_.size).sum * -1
      }
    })
    val emptyVal = if (sorted.nonEmpty) {
      val head = sorted.head
      val tail = sorted.tail
      parSides
        .find(s => currentLoads(s) == BigDecimal(0))
        .map(s => {
          val heuristic = new QueryCentricPartitioningHeuristic()
          val nFragmentAllocation = fragmentAllocation.updated(s,
            fragmentAllocation(s).union((head +: updates(head, updateApplications)).flatMap(_.fragments)).distinct)
          if (restWeight(head) > scaledLoads(s) - currentLoads(s)) {
            val nRestWeight = restWeight.updated(head, restWeight(head) - (scaledLoads(s) - currentLoads(s)))
            val nCurrentLoads = currentLoads.updated(s, scaledLoads(s))
            heuristic.continueDistribution(head +: tail, sides, nCurrentLoads, scaledLoads, nRestWeight, nFragmentAllocation)
              .view.map(_._2.view.map(_.size).sum).sum
          } else {
            val nCurrentLoads = currentLoads.updated(s, currentLoads(s) + restWeight(head))
            heuristic.continueDistribution(tail, sides, nCurrentLoads, scaledLoads, restWeight, nFragmentAllocation)
              .map(_._2.map(_.size).sum).sum
          }
        })
    } else None
    sorted match {
      case head :: tail =>
        //select backend with minimal difference
        val side = parSides.minBy {
          case s if currentLoads(s) == BigDecimal(0) => emptyVal.getOrElse(0l)//0l
          case s if currentLoads(s) == scaledLoads(s) => Long.MaxValue
          case s =>
            val heuristic = new QueryCentricPartitioningHeuristic()
            val nFragmentAllocation = fragmentAllocation.updated(s,
              fragmentAllocation(s).union((head +: updates(head, updateApplications)).flatMap(_.fragments)).distinct)
            if (restWeight(head) > scaledLoads(s) - currentLoads(s)) {
              val nRestWeight = restWeight.updated(head, restWeight(head) - (scaledLoads(s) - currentLoads(s)))
              val nCurrentLoads = currentLoads.updated(s, scaledLoads(s))
              heuristic.continueDistribution(head :: tail, sides, nCurrentLoads, scaledLoads, nRestWeight, nFragmentAllocation)
                  .view.map(_._2.view.map(_.size).sum).sum
            } else {
              val nCurrentLoads = currentLoads.updated(s, currentLoads(s) + restWeight(head))
              heuristic.continueDistribution(tail, sides, nCurrentLoads, scaledLoads, restWeight, nFragmentAllocation)
                .map(_._2.map(_.size).sum).sum
            }
        }
        //println(s"put ${head.name} into ${s.name}")
        val nFragmentAllocation = fragmentAllocation.updated(side,
          fragmentAllocation(side).union((head +: updates(head, updateApplications)).flatMap(_.fragments)).distinct)
        if (restWeight(head) > scaledLoads(side) - currentLoads(side)) {
          val nRestWeight = restWeight.updated(head, restWeight(head) - (scaledLoads(side) - currentLoads(side)))
          val nCurrentLoads = currentLoads.updated(side, scaledLoads(side))
          recDistribute(head :: tail, updateApplications, sides, nCurrentLoads, scaledLoads, nRestWeight, nFragmentAllocation)
        } else {
          val nCurrentLoads = currentLoads.updated(side, currentLoads(side) + restWeight(head))
          recDistribute(tail, updateApplications, sides, nCurrentLoads, scaledLoads, restWeight, nFragmentAllocation)
        }
      case _ =>
        fragmentAllocation
    }
  }

  override def distribute(applications: Seq[Application], sides: Seq[Side]): collection.Map[Side, Seq[Fragment]] = {
    val input = applications
    val currentLoads = sides.map(s => (s, BigDecimal(0))).toMap
    val scaledLoads = sides.map(s => (s, s.capacity)).toMap
    val restWeight = applications.map(a => (a, a.load)).toMap
    val fragmentAllocation = sides.map(s => (s, Seq.empty[Fragment])).toMap

    recDistribute(input, Seq.empty, sides, currentLoads, scaledLoads, restWeight, fragmentAllocation)
  }
}
