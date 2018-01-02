package de.hpi.epic.partialTableDistribution.algorithms.heuristics

import de.hpi.epic.partialTableDistribution.data.{Application, Fragment, Side}

import scala.annotation.tailrec

/**
  * Created by Jan on 25.09.2017.
  */
class MinimumHeuristic extends Heuristic {
  private def fragmentUsage(applications: Seq[Application]): Map[Fragment, Int] =
    applications.foldLeft(Map.empty[Fragment, Int])((m1, a) => a.fragments.foldLeft(m1)((m2, c) => m2.updated(c, m2.getOrElse(c, 0) + 1)))

  private def singleUsedFragments(applications: Seq[Application]): Seq[Fragment] = fragmentUsage(applications).filter(_._2 == 1).keySet.toSeq

  private def weightedJaccardDistance(s1: Seq[Fragment], s2: Seq[Fragment]): Double = {
    val intersection = s1.intersect(s2).foldLeft(0l)(_ + _.size)
    val union = s1.union(s2).distinct.foldLeft(0l)(_ + _.size)
    return intersection / union.toDouble
  }

  protected def similarityMatrix(applications: Seq[Application]):Matrix[Double] = {
    val suf = singleUsedFragments(applications)
    applications.map(a1 => {
      val fc1 = a1.fragments.diff(suf)
      applications.map(a2 => {
        val fc2 = a2.fragments.diff(suf)
        weightedJaccardDistance(fc1, fc2)
      }).toArray
    }).toArray
  }

  private def addingCosts(side: Side, distribution: Map[Side, Seq[Fragment]], application: Application): Long = {
    if (distribution(side).isEmpty) 0
    else application.fragments.diff(distribution(side)).map(_.size).sum
  }

  @tailrec
  private def recDistribute(applications: Seq[Application], sides: Seq[Side],
                            remainingWeight: Map[Application, BigDecimal], load: Map[Side, BigDecimal],
                            distribution: Map[Side, Seq[Fragment]]): Map[Side, Seq[Fragment]] = {
    applications match {
      case l@(head :: tail) =>
        val remainingSides = sides.filter(s => s.capacity > load(s))
        val (side, _) = remainingSides.map(s => (s, addingCosts(s, distribution, head))).minBy(_._2)
        if (side.capacity - load(side) >= remainingWeight(head)) {
          val ul = load.updated(side, load(side) + remainingWeight(head))
          val ud = distribution.updated(side, (distribution(side) ++ head.fragments).distinct)
          recDistribute(tail, sides, remainingWeight, ul, ud)
        } else {
          val uw = remainingWeight.updated(head, remainingWeight(head) - (side.capacity - load(side)))
          val ul = load.updated(side, side.capacity)
          val ud = distribution.updated(side, (distribution(side) ++ head.fragments).distinct)
          recDistribute(l, sides, uw, ul, ud)
        }
      case _ => distribution
    }
  }

  def distribute(applications: Seq[Application], sides: Seq[Side]) = {
    val matrix = similarityMatrix(applications)
    val input = applications.zipWithIndex.sortBy(t => matrix(t._2).sum - 1).map(_._1)

    //distribute weakest directly
    val dist = sides.map((_, Seq.empty[Fragment])).toMap
    val load = sides.map((_, BigDecimal(0))).toMap
    val remainingWeight = input.map(a => (a, a.load)).toMap
    recDistribute(input, sides, remainingWeight, load, dist)
  }
}
