package de.hpi.epic.partialTableDistribution.algorithms.heuristics
import de.hpi.epic.partialTableDistribution.data.{Application, Fragment, Side}

import scala.annotation.tailrec

/**
  * Created by Jan on 02.10.2017.
  */
class TestAllHeuristic extends Heuristic {
  private def similarityMatrix(applications: Seq[Application]): Matrix[Double] = {
    applications.map(a1 => applications.map(a2 => weightedJaccardDistance(a1.fragments.toSet, a2.fragments.toSet)).toArray).toArray
  }

  private def weightedJaccardDistance(s1: Set[Fragment], s2: Set[Fragment]): Double = {
    val intersection = s1.intersect(s2).foldLeft(0l)(_ + _.size)
    val union = s1.union(s2).foldLeft(0l)(_ + _.size)
    return intersection / union.toDouble
  }

  private def addingCosts(application: Application, side: XSide): Long = {
    val fragments = application.fragments.diff(side.apps.flatMap(_.fragments))
    fragments.map(_.size).sum
  }

  private def calculateNext(selected: Seq[Application], remaining: Seq[Application]) =
    remaining.minBy(r => selected.map(s => weightedJaccardDistance(r.fragments.toSet, s.fragments.toSet)).sum)

  private def appRest(application: Application, side: XSide): Application = application match {
    case xa: XApplication => xa.partialLoad(xa.load - (side.capacity - side.load))
    case _ => new XApplication(application).partialLoad(application.load - (side.capacity - side.load))
  }

  private def start(applications: Seq[Application], sides: Seq[XSide]): Seq[XSide] = {
    val m = similarityMatrix(applications)
    val (index, _) = m.zipWithIndex.map(t => (t._2, t._1.sum - 1)).minBy(_._2)
    val e = applications(index)
    //recStart(Seq(e), applications.filterNot(_ == e), Seq(sides.head.append(e)), sides.tail)
    if (sides.head.fits(e)) {
      recStart(Seq(e), applications.filter(_ != e), Seq(sides.head.append(e)), sides.tail)
    } else {
      recStart(Seq(e), applications.updated(applications.indexOf(e), appRest(e, sides.head)), Seq(sides.head.fill(e)), sides.tail)
    }
  }

  @tailrec
  private def recStart(selectedApplications: Seq[Application], remainingApplications: Seq[Application],
                       filledSides: Seq[XSide], remainingSides: Seq[XSide]): Seq[XSide] =
    remainingSides match {
      case head :: tail =>
        val e = calculateNext(selectedApplications, remainingApplications)
        if (head.fits(e)) {
          recStart(selectedApplications :+ e, remainingApplications.filter(_ != e), filledSides :+ head.append(e), tail)
        } else {
          recStart(selectedApplications, remainingApplications.updated(remainingApplications.indexOf(e), appRest(e, head)), filledSides :+ head.fill(e), tail)
        }
      case _ => recDistribute(remainingApplications, filledSides)
    }

  @tailrec
  private def recDistribute(applications: Seq[Application], sides: Seq[XSide]): Seq[XSide] = {
    if (applications.isEmpty) {
      sides
    } else {
      val (application, side, aIndex, sIndex) = applications.zipWithIndex.flatMap(a => sides.zipWithIndex.filter(t => t._1.load < t._1.capacity).map(s => (a._1, s._1, a._2, s._2))).minBy(t => addingCosts(t._1, t._2))
      if (side.fits(application)) {
        recDistribute(applications.filter(_ != application), sides.updated(sIndex, side.append(application)))
      } else {
        val updatedApplication = new XApplication(application).partialLoad(application.load - (side.capacity - side.load))
        recDistribute(applications.updated(aIndex, updatedApplication), sides.updated(sIndex, side.fill(application)))
      }
    }
  }

  override def distribute(applications: Seq[Application], sides: Seq[Side]): Map[Side, Seq[Fragment]] =
    start(applications, sides.map(s => new XSide(Seq(s))).toList)
    .map(xside => (xside.bases.head, xside.apps.flatMap(_.fragments).distinct))
    .toMap
}
