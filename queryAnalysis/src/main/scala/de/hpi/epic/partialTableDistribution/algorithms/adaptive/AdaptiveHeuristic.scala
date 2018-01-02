package de.hpi.epic.partialTableDistribution.algorithms.adaptive

import util.control.Breaks._

import de.hpi.epic.partialTableDistribution.algorithms.heuristics.{Allocation, QueryCentricPartitioningHeuristic}
import de.hpi.epic.partialTableDistribution.data._
import de.hpi.epic.partialTableDistribution.utils.{Tabulator, toByteConverter}

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.collection.{mutable, _}
import scala.math.BigDecimal.RoundingMode

/**
  * Created by Jan on 08.11.2017.
  */
object AdaptiveHeuristic {
  var nextId = 0

  def sameApplication(newApp: Application, oldApp: Application): Boolean = newApp.name == oldApp.name
  def differences(oldWl: Seq[Application], newWl: Seq[Application]): Seq[Change] = {
    val changes1 = newWl.flatMap(app => {
      oldWl.find(sameApplication(app, _)) match {
        case None => Some(NewApplication(app))
        case Some(a2) if a2.load != app.load => Some(ChangedWeight(app, a2, app.load - a2.load))
        case _ => None
      }
    })
    val changes2 = oldWl collect {
      case app if !newWl.exists(sameApplication(app, _)) => RemovedApplication(app)
    }
    changes1 ++ changes2
  }

  private def debugInfo(changes: Seq[Change]): Unit = {
    changes.foreach{
      case NewApplication(a) => println(s"new application: ${a.name}")
      case RemovedApplication(a) => println(s"removed application ${a.name}")
      case ChangedWeight(a,_,w) => println(s"changed weigth ${a.name} with difference $w")
    }
  }

  @tailrec
  private def recursiveOverlapExtraction(changes: List[ChangedWeight], remainingOverlap: BigDecimal,
                                         resultingChanges: List[Change]): List[Change] =
    if (remainingOverlap > 0) {
      recursiveOverlapExtraction(
        changes.tail,
        remainingOverlap - changes.head.newApp.load,
        RemovedApplication(changes.head.oldApp) +: (NewApplication(changes.head.newApp) +: resultingChanges)
      )
    } else {
      resultingChanges ++ changes
    }

  //change overlap to NewApplication
  private def overlapExtraction(changes: List[Change], share: Map[Application, BigDecimal]): List[Change] = {
    val overlap = changes.map {
      case RemovedApplication(app) => -share(app)
      case ChangedWeight(_, _, diff) => diff
    }.sum
    if (overlap > 0) {
      val (weightChange, rest) = changes.partition(p => p.isInstanceOf[ChangedWeight] && p.loadDiff > BigDecimal(0))
      recursiveOverlapExtraction(weightChange.asInstanceOf[List[ChangedWeight]], overlap, rest)
    } else changes
  }

  def calculateDistribution(oldWl: Seq[Application],
                            newWl: Seq[Application],
                            distribution: Map[Side, Map[Application, BigDecimal]],
                            sideCapacity: BigDecimal
                           ) = {
    val original_changes = differences(oldWl, newWl)
    debugInfo(original_changes)

    //1st step: ChangedWeight of splitted applications replaced by removals and additions
    val changes = original_changes.flatMap{
      case ChangedWeight(newApp, oldApp, _) if distribution.values.count(_.contains(oldApp)) > 1 => Seq(RemovedApplication(oldApp), NewApplication(newApp))
      case change => Seq(change)
    }
    val (additions, updates) = changes.partition(_.isAddition).asInstanceOf[(Seq[NewApplication], Seq[Change])]

    //2nd step: assign changes to sides
    val changesPerSide = updates.foldLeft(new HashMap[Side, List[Change]]()) ((res, cur) => {
      val app = cur match {
        case RemovedApplication(a) => a
        case ChangedWeight(_, a, _) => a
      }
      val sides = distribution.view.filter(t => t._2.contains(app)).map(_._1).toList
      sides.foldLeft(res)((m, s) => m + ((s, cur :: res.getOrElse(s, Nil))) )
    })

    //3rd step: overlap elimination
    val updatedChanges = changesPerSide.map(t => (t._1, overlapExtraction(t._2, distribution(t._1))))
    val updatedAdditions = updatedChanges.flatMap(_._2.filter(_.isAddition).asInstanceOf[List[NewApplication]])

    //4th step: update distribution with removals and changed workload
    val updatedDistribution = mutable.Map(distribution.toSeq :_*)
    updatedChanges.foreach(t => {
      val (side, changes) = t
      changes.foreach{
        case RemovedApplication(app) => updatedDistribution.update(side, updatedDistribution(side) - app)
        case ChangedWeight(_, app, diff) =>
          val oldEntry = updatedDistribution(side)
          updatedDistribution.update(side, oldEntry.updated(app, oldEntry(app) + diff))
        case _ =>
      }
    })
    printLoadMatrix(updatedDistribution)

    //replace old apps with new apps in distribution
    oldWl.foreach(oldQuery => {
      val oNewQuery = newWl.find(_.name == oldQuery.name)
      oNewQuery.foreach(newQuery => {
        val values = updatedDistribution.filter(_._2.contains(oldQuery)).map(t => {
          (t._1, t._2(oldQuery))
        })
        values.foreach(t => {
          val mapping = updatedDistribution(t._1)
          updatedDistribution.update(t._1, (mapping - oldQuery).updated(newQuery, t._2))
        })
      })
    })

    //5th step: adjust node amount
    val totalCapacity = distribution.keys.foldLeft(BigDecimal(0))(_ + _.capacity)
    val totalLoad = newWl.map(_.load).sum
    val exactDiff = totalCapacity - totalLoad
    var furtherAdditions = Seq.empty[NewApplication]
    //val total_diff = original_changes.foldLeft(BigDecimal(0))(_ + _.loadDiff)
    if (exactDiff < 0) {
      // add new nodes
      val newSidesCount = (exactDiff.abs / sideCapacity).setScale(0, RoundingMode.CEILING).toInt
      (1 to newSidesCount).foreach(i => {
        val side = new Side {
          val id = nextId
          override def capacity: BigDecimal = sideCapacity
          override def name: String = s"newS$id"
        }
        nextId += 1
        updatedDistribution.update(side, Map.empty)
      })
    } else if (exactDiff > 0) {
      //remove old nodes
      var removedCapacity = BigDecimal(0)
      breakable {
        while (true) {
          val sidesForRemoval = updatedDistribution.filter(t => t._1.capacity <= (exactDiff - removedCapacity))
          println("removed")
          if (sidesForRemoval.nonEmpty) {
            val (side, assignedApps) = sidesForRemoval.minBy(_._2.foldLeft(BigDecimal(0))(_ + _._2))
            furtherAdditions = furtherAdditions ++ assignedApps.keys.map(NewApplication)
            updatedDistribution -= side
            removedCapacity += side.capacity
          } else {
            break
          }
        }
      }
    }

    //6th step: distribute added queries to replicas not filled
    val newApplications = (additions ++ updatedAdditions /*++ furtherAdditions*/).map(_.newApp)
    println(newApplications.map(_.name).mkString(", "))
    val heuristic = new QueryCentricPartitioningHeuristic()
    val dist = heuristic.continue(Allocation(updatedDistribution), newApplications)
    printLoadMatrix(dist.loadMap)
    dist
  }

  //Test case
  def main(args: Array[String]): Unit = {
    case class Backend(capacity: BigDecimal, name: String) extends Side
    val sides = IndexedSeq(Backend(BigDecimal(0.4), "S1"), Backend(BigDecimal(0.3), "S2"), Backend(BigDecimal(0.3), "S3"))

    import de.hpi.epic.partialTableDistribution.data.Table

    val tables = Map("A" -> Table("A", Seq(Column("A1", 1, 11))), "B" -> Table("B", Seq(Column("B1", 1, 11))), "C" -> Table("C", Seq(Column("C1", 1, 11))))
    val oldWl: Seq[Query] = Seq(
      Query("Q1", Seq(tables("A")), BigDecimal(0.24d)),
      Query("Q2", Seq(tables("B")), BigDecimal(0.2d)),
      Query("Q3", Seq(tables("C")), BigDecimal(0.3d)),
      Query("Q4", Seq(tables("A"), tables("B")), BigDecimal(0.26d))
    )
    val heuristic = new QueryCentricPartitioningHeuristic()
    heuristic.distribute(oldWl, sides)
    val distribution = Allocation.transform(heuristic.getLoadMatrix)
    printLoadMatrix(distribution.loadMap)
    val newWl: Seq[Query] = Seq(
      Query("Q1", Seq(tables("A")), BigDecimal(0.16d)),
      Query("Q2", Seq(tables("B")), BigDecimal(0.24d)),
      Query("Q3", Seq(tables("C")), BigDecimal(0.3d))
    )
    val res = calculateDistribution(oldWl, newWl, distribution.loadMap, BigDecimal("0.2"))
    costs(new LocalLogCostModel(227), distribution, res)
    println(res.fragmentMap.view.map(_._2.distinct.foldLeft(0l)(_ + _.size)).sum)
    println("========================================================")

    heuristic.distribute(newWl, sides.take(2))
    val qcpResultNew = Allocation.transform(heuristic.getLoadMatrix)
    printLoadMatrix(qcpResultNew.loadMap)
    costs(new LocalLogCostModel(227), distribution, qcpResultNew)
    println(qcpResultNew.fragmentMap.view.map(_._2.distinct.foldLeft(0l)(_ + _.size)).sum)
  }

  def costs(model:CostModel, oldDist: Allocation, newDist: Allocation): Unit = {
    val costs = model.transmissionCosts(oldDist.fragmentMap, newDist.fragmentMap)
    println(s"real time: ${costs.realTime}s\nproc time: ${costs.procTime}s")
  }

  def printLoadMatrix(loadMatrix: Map[Side, Map[Application, BigDecimal]]) = {
    //extract table keys
    val colKeys = loadMatrix.values.flatMap(_.keys).toSeq.distinct.sortBy(_.name)
    val rowKeys = loadMatrix.keys.toSeq.sortBy(_.name)

    val header = "" +: colKeys.map(_.name) :+ "Overall"
    val rows = rowKeys.map(rk => {
      val values = colKeys.map(ck => loadMatrix(rk).getOrElse(ck, BigDecimal(0)))
      val res = values.sum
      rk.name +: (values :+ res).map(v => (v * BigDecimal(100)).toString() + "%")
    })
    val result = Tabulator.format(header +: rows)
    println(result)
  }
}
