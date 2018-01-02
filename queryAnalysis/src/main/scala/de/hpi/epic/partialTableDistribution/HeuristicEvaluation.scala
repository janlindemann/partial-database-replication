package de.hpi.epic.partialTableDistribution

import java.io.{BufferedWriter, File, FileWriter}

import de.hpi.epic.partialTableDistribution.algorithms.heuristics.Heuristic
import de.hpi.epic.partialTableDistribution.data.Side
import de.hpi.epic.partialTableDistribution.utils.{timeAnd, timeOnly}

/**
  * Created by Jan on 16.12.2017.
  */
trait HeuristicEvaluation {
  self: Workload =>

  def evaluate(heuristic: Heuristic, backendCount: Int, baseSize: Long, runs: Int = 1): (Double, Long, Long, Double) = {
    val sides = (1 to backendCount) map (i => new Side {
      override def capacity: BigDecimal = BigDecimal(1) / BigDecimal(backendCount)
      override def name: String = s"B$i"
    })
    val (runtimeFirst, allocation) = timeAnd(heuristic.distribute(queries, sides))
    val runtimes = runtimeFirst +: (1 until runs).map(_ => timeOnly(heuristic.distribute(queries, sides)))
    val runtime = runtimes.sum / runtimes.length
    val sizes = allocation.map(t => (t._1, t._2.map(_.size).sum))
    val memorySize = sizes.values.sum
    val replicationFactor = memorySize / baseSize.toDouble
    val mean = memorySize / sizes.size.toDouble
    val stddev = Math.sqrt(sizes.view.map(t => Math.pow(t._2 - mean, 2)).sum / sizes.size.toDouble)

    (replicationFactor, memorySize, runtime, stddev)
  }

  def analyzes(heuristics: Seq[Heuristic], path: String, start: Int, end: Int): Unit = {
    val regex = ".(\\w+)[$]?$".r
    val singleSize = queries.view.flatMap(_.fragments).distinct.map(_.size).sum
    heuristics.foreach(h => {
      val heuristicName = regex.findFirstMatchIn(h.getClass.toString).get.subgroups.head
      val completePath = if (path.endsWith("/")) path + heuristicName + ".csv" else  path + "/" + heuristicName + ".csv"
      val file = new File(completePath)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("backendCount\treplication_rate\tmemory_size\truntime\tstddev\n")
      (start to end).foreach(backendCount => {
        val (replRate, memSize, runtime, stddev) = evaluate(h, backendCount, singleSize)
        bw.write(s"$backendCount\t$replRate\t$memSize\t$runtime\t$stddev\n")
        bw.flush()
      })
      bw.close()
    })
  }

}
