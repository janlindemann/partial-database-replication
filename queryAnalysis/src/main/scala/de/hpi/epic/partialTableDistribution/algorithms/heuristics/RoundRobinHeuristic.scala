package de.hpi.epic.partialTableDistribution.algorithms.heuristics
import de.hpi.epic.partialTableDistribution.data.{Application, Fragment, Side}

import scala.annotation.tailrec
import scala.collection._

/**
  * Created by Jan on 17.10.2017.
  */
object RoundRobinHeuristic extends Heuristic {
  @tailrec
  def recDistribute(remaining: Seq[Application], sides: Seq[Side], remainingLoad: Map[Application, BigDecimal],
                    assignedLoad: Map[Side, BigDecimal], distribution: Map[Side, Seq[Fragment]], nextIndex: Int
                   ): Map[Side, Seq[Fragment]] = {
    remaining match {
      case head :: tail =>
        val remainingSides = sides.filter(s => assignedLoad(s) < s.capacity)
        if (remainingSides.length == 0)
          return distribution
        val nextSide = remainingSides(nextIndex % remainingSides.length)
        //println(s"${head.name} with load ${remainingLoad(head)} assigned to ${nextSide.name}")
        if (remainingLoad(head) <= (nextSide.capacity - assignedLoad(nextSide))) {
          val newLoad = assignedLoad(nextSide) + remainingLoad(head)
          val assignedFrags = (distribution(nextSide) ++ head.fragments).distinct
          recDistribute(tail, sides, remainingLoad, assignedLoad.updated(nextSide, newLoad),
            distribution.updated(nextSide, assignedFrags), nextIndex + 1)
        } else {
          val newLoad = remainingLoad(head) - (nextSide.capacity - assignedLoad(nextSide))
          val assignedFrags = (distribution(nextSide) ++ head.fragments).distinct
          recDistribute(head :: tail, sides, remainingLoad.updated(head, newLoad), assignedLoad.updated(nextSide, nextSide.capacity),
            distribution.updated(nextSide, assignedFrags), nextIndex + 1)
        }
      case _ => distribution
    }
  }

  override def distribute(applications: Seq[Application], sides: Seq[Side]): Map[Side, Seq[Fragment]] = {
    recDistribute(applications, sides, applications.map(a => (a, a.load)).toMap,
      sides.map(s => (s, BigDecimal(0))).toMap, sides.map(s => (s, Seq.empty[Fragment])).toMap, 0)
  }
}
