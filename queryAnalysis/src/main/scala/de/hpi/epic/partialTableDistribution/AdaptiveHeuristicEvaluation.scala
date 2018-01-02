package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.adaptive.{AdaptiveHeuristic, CostModel, LocalLogCostModel, TransmissionCost}
import de.hpi.epic.partialTableDistribution.algorithms.heuristics.{Allocation, Heuristic, QueryCentricPartitioningHeuristic}
import de.hpi.epic.partialTableDistribution.data.{Application, Fragment, Side}
import de.hpi.epic.partialTableDistribution.utils.{CSVFile, Tabulator}

/**
  * Created by Jan on 17.12.2017.
  */
trait AdaptiveHeuristicEvaluation {
  self: AdaptiveWorkload =>

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

  def costs(model:CostModel, oldDist: Allocation, newDist: Allocation): Unit = {
    val costs = model.transmissionCosts(oldDist.fragmentMap, newDist.fragmentMap)
    println(s"real time: ${costs.realTime}s\nproc time: ${costs.procTime}s")
  }

  def evaluateStaticHeuristic(heuristic: Heuristic, sideCount: Seq[Int], baseCapacity: BigDecimal, path: String) = {
    val resPath = if (path.endsWith("/")) path else path + "/"
    val backends = (1 to sideCount.max)  map (i => new Side {
      override def capacity: BigDecimal = baseCapacity
      override def name: String = s"B$i"
    })

    val costModel = new LocalLogCostModel(227000000)

    var previousAllocation: Option[Map[Side, Seq[Fragment]]] = None
    val data = queries.zip(sideCount).zipWithIndex.map(t => {
      val ((wl, sc), index) = t
      val allocation = heuristic.distribute(wl, backends.take(sc))
      val resultSize = allocation.view.map(_._2.foldLeft(0l)(_ + _.size)).sum
      val minimumSize = wl.flatMap(_.fragments).distinct.foldLeft(0l)(_ + _.size)
      val replicationFactor = resultSize / minimumSize.toDouble
      val byteDiff = previousAllocation.map(pa => costModel.byteDiff(pa, allocation)).getOrElse(resultSize)
      val transferFactor = byteDiff / minimumSize.toDouble
      val costs = previousAllocation.map(pa => costModel.transmissionCosts(pa, allocation)).getOrElse(TransmissionCost(0, 0))
      previousAllocation = Some(allocation.toMap)
      Array(index.toString, resultSize.toString, replicationFactor.toString, byteDiff.toString, transferFactor.toString, costs.realTime.toString, costs.procTime.toString)
    }).toArray

    val file = new CSVFile(data, Array("run", "size", "replication_factor", "byte_diff", "transfer_factor", "real_time", "proc_time"))
    file.save(s"${resPath}QCP.csv", "\t")
  }

  def evaluateDynamicHeuristic(maxSides: Int, baseCapacity: BigDecimal, path: String): Unit = {
    val resPath = if (path.endsWith("/")) path else path + "/"
    val backends = (1 to maxSides)  map (i => new Side {
      override def capacity: BigDecimal = baseCapacity
      override def name: String = s"B$i"
    })

    val costModel = new LocalLogCostModel(227000000)

    //use arbitrary heuristic as start point
    val heuristic = new QueryCentricPartitioningHeuristic()
    heuristic.distribute(queries(0), backends)
    var previousAllocation = Allocation.transform(heuristic.getLoadMatrix)
    var previousWorkload = queries(0)

    val baseSize = previousAllocation.fragmentMap.view.map(_._2.foldLeft(0l)(_ + _.size)).sum

    //actual loop + static first row
    val data =
      Array(baseSize.toString, baseSize.toString, "0", "0") +:
        queries.zipWithIndex.tail.map(t => {
          val (wl, index) = t
          val allocation = AdaptiveHeuristic.calculateDistribution(previousWorkload, wl, previousAllocation.loadMap, baseCapacity)
          val resultSize = allocation.fragmentMap.view.map(_._2.foldLeft(0l)(_ + _.size)).sum
          val minimumSize = wl.flatMap(_.fragments).distinct.foldLeft(0l)(_ + _.size)
          val replicationFactor = resultSize / minimumSize.toDouble
          val byteDiff = costModel.byteDiff(previousAllocation.fragmentMap, allocation.fragmentMap)
          val transferFactor = byteDiff / minimumSize.toDouble
          val costs = costModel.transmissionCosts(previousAllocation.fragmentMap, allocation.fragmentMap)
          previousAllocation = allocation
          previousWorkload = wl
          Array(index.toString, resultSize.toString, replicationFactor.toString, byteDiff.toString, transferFactor.toString, costs.realTime.toString, costs.procTime.toString)
        }).toArray

    val file = new CSVFile(data, Array("run", "size", "replication_factor", "byte_diff", "transfer_factor", "real_time", "proc_time"))
    file.save(s"${resPath}Adaptive.csv", "\t")
  }
}
