package de.hpi.epic.partialTableDistribution.algorithms.heuristics

import de.hpi.epic.partialTableDistribution.data._
import de.hpi.epic.partialTableDistribution.utils.Tabulator

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection._

/**
  * Created by Jan on 20.09.2017.
  */
class QueryCentricPartitioningHeuristic extends Heuristic with WithContinue {
  val loadMatrix = mutable.Map.empty[(Side, Application), BigDecimal]

  def getLoadMatrix: Map[(Side, Application), BigDecimal] = loadMatrix.toMap
  def printLoadMatrix: Unit = {
    //extract table keys
    val colKeys = loadMatrix.keys.map(_._2).toSeq.distinct.sortBy(_.name)
    val rowKeys = loadMatrix.keys.map(_._1).toSeq.distinct.sortBy(_.name)

    val header = "" +: colKeys.map(_.name) :+ "Overall"
    val rows = rowKeys.map(rk => {
      val values = colKeys.map(ck => loadMatrix.getOrElse((rk, ck), BigDecimal(0)))
      val res = values.sum
      rk.name +: (values :+ res).map(v => (v * BigDecimal(100)).toString() + "%")
    })
    val result = Tabulator.format(header +: rows)
    println(result)
  }

  private def classify(applications: Seq[Application]): (Seq[Application], Seq[Application]) = applications.partition(_.classification == ReadQuery)

  private def updates(application: Application, updateApplications: Seq[Application]) =
    updateApplications.filter(uApp => uApp.fragments.intersect(application.fragments).nonEmpty)

  //sum of weight of update queries that should already be assigned to backend
  private def updateWeight(application: Application, side: Side, updateApplications: Seq[Application],
                           fragmentAllocation: Map[Side, Seq[Fragment]]): BigDecimal = {
    updates(application, updateApplications)
      .map(updateApplication => {
        if (fragmentAllocation(side).intersect(updateApplication.fragments).nonEmpty) updateApplication.load else BigDecimal(0)
      })
      .sum
  }

  def continue(distribution: Allocation, remaining: Seq[Application]): Allocation = {
    loadMatrix.clear()

    val sides = distribution.loadMap.keys.toList

    val currentLoads = mutable.Map(
      distribution.loadMap.map(t => (t._1, t._2.values.sum)).toSeq :_*
    )
    val scaledLoads = mutable.Map(sides.map(s => (s, s.capacity)) : _*)
    val restWeight = mutable.Map(remaining.map(r => (r, r.load)) :_*)
    val fragmentAllocation = mutable.Map( distribution.fragmentMap.toSeq :_* )

    val existing = distribution.loadMap.values.toSeq.view.flatMap(_.keys).distinct.force
    for (
      i <- sides;
      j <- existing ++ remaining
    ) loadMatrix.update((i,j), BigDecimal(0))
    distribution.loadMap.foreach(t => t._2.foreach(e => loadMatrix.update((t._1, e._1), e._2)))

    recDistribute(remaining, Seq.empty, sides, currentLoads, scaledLoads, restWeight, fragmentAllocation)

    Allocation(loadMatrix.foldLeft(Map.empty[Side, Map[Application, BigDecimal]])((m, t) =>
      if (t._2 > BigDecimal(0))
        m.updated(t._1._1, m.getOrElse(t._1._1, Map.empty[Application, BigDecimal]).updated(t._1._2, t._2))
      else
        m
    ))
  }

  def continueDistribution(remaining: Seq[Application], sides: Seq[Side], currentLoads: Map[Side, BigDecimal],
                           scaledLoads: Map[Side, BigDecimal], restWeight: Map[Application, BigDecimal],
                           fragmentAllocation: Map[Side, Seq[Fragment]]): Map[Side, Seq[Fragment]] = {
    loadMatrix.clear()
    for {
      i <- sides;
      j <- remaining
    } loadMatrix.update((i,j), BigDecimal(0))

    recDistribute(remaining, Seq.empty, sides, mutable.Map(currentLoads.toSeq :_*), mutable.Map(scaledLoads.toSeq :_*),
      mutable.Map(restWeight.toSeq :_*), mutable.Map(fragmentAllocation.toSeq :_*))
  }

  def distribute(applications: Seq[Application], sides: Seq[Side]): Map[Side, Seq[Fragment]] = {
    val (readApplications, updateApplications) = classify(applications)
    val standaloneUpdates = updateApplications.filter(uApp => {
      !readApplications.exists(rApp => uApp.fragments.intersect(rApp.fragments).nonEmpty)
    })
    val input = readApplications union standaloneUpdates
    val currentLoads = mutable.Map(sides.map(s => (s, BigDecimal(0))) : _*)
    val scaledLoads = mutable.Map(sides.map(s => (s, s.capacity)) : _*)
    val restWeight = mutable.Map(applications.map(a => (a, a.load)) : _*)
    val fragmentAllocation = mutable.Map(sides.map(s => (s, Seq.empty[Fragment])) : _*)

    //for debugging purposes
    loadMatrix.clear()
    for {
      i <- sides;
      j <- applications
    } loadMatrix.update((i,j), BigDecimal(0))

    recDistribute(input, updateApplications, sides, currentLoads, scaledLoads, restWeight, fragmentAllocation)
  }

  @tailrec
  private def recDistribute(remaining: Seq[Application],
                            updateApplications: Seq[Application],
                            sides: Seq[Side],
                            currentLoads: mutable.Map[Side, BigDecimal],
                            scaledLoads: mutable.Map[Side, BigDecimal],
                            restWeight: mutable.Map[Application, BigDecimal],
                            fragmentAllocation: mutable.Map[Side, Seq[Fragment]]): Map[Side, Seq[Fragment]] = {
    remaining.sortBy(app => {
      if (app.load == restWeight(app)) {
        val appGroup = app +: updates(app, updateApplications)
        appGroup.foldLeft(BigDecimal(0))(_ + _.load) * appGroup.flatMap(_.fragments).distinct.map(_.size).sum * -1
      } else {
        restWeight(app) * app.fragments.map(_.size).sum * -1
      }

    }) match {
      case head :: tail =>
        //if all backends are full, update scaled loads
        if (!sides.exists(s => currentLoads(s) < scaledLoads(s)))
          currentLoads.foreach(l => scaledLoads.update(l._1, l._2 + l._1.capacity * head.load))

        //select backend with minimal difference
        val s = sides.minBy {
          case s if currentLoads(s) == BigDecimal(0) => BigDecimal(0)
          case s if currentLoads(s) == scaledLoads(s) => BigDecimal(Double.MaxValue)
          case s =>
            (head +: updates(head, updateApplications))
              .flatMap(_.fragments)
              .distinct
              .diff(fragmentAllocation(s))
              .foldLeft(BigDecimal(0))((l, r) => l + r.size)
        }

        //println(s.name, head.name, updates(head, updateApplications).map(_.name).mkString(" & "))

        //update loads and fragment allocation
        currentLoads.update(s, currentLoads(s) +
          updates(head, updateApplications).map(_.load).sum - updateWeight(head, s, updateApplications, fragmentAllocation))
        updates(head, updateApplications).foreach(u => loadMatrix.update((s, u), u.load))
        fragmentAllocation.update(s,
          fragmentAllocation(s).union((head +: updates(head, updateApplications)).flatMap(_.fragments)).distinct)

        if (updateApplications.contains(head)) {
          if (currentLoads(s) > scaledLoads(s))
            scaledLoads.update(s, currentLoads(s))
          recDistribute(tail, updateApplications, sides, currentLoads, scaledLoads, restWeight, fragmentAllocation)
        } else {
          if (currentLoads(s) >= scaledLoads(s))
            scaledLoads.update(s, currentLoads(s) + s.capacity * head.load)
          if (restWeight(head) > scaledLoads(s) - currentLoads(s)) {
            loadMatrix.update((s, head), loadMatrix(s, head) + (scaledLoads(s) - currentLoads(s)))
            restWeight.update(head, restWeight(head) - (scaledLoads(s) - currentLoads(s)))
            currentLoads.update(s, scaledLoads(s))
            recDistribute(head :: tail, updateApplications, sides, currentLoads, scaledLoads, restWeight, fragmentAllocation)
          } else {
            loadMatrix.update((s, head), loadMatrix(s, head) + restWeight(head))
            currentLoads.update(s, currentLoads(s) + restWeight(head))
            recDistribute(tail, updateApplications, sides, currentLoads, scaledLoads, restWeight, fragmentAllocation)
          }
        }
      case _ =>
        fragmentAllocation
    }
  }
}
