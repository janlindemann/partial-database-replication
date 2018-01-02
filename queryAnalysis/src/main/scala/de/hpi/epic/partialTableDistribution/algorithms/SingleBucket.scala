package de.hpi.epic.partialTableDistribution.algorithms

import de.hpi.epic.partialTableDistribution.data.Query
import de.hpi.epic.partialTableDistribution.utils
import de.hpi.epic.partialTableDistribution.utils.TreeCombinations
import de.hpi.epic.partialTableDistribution.utils.TreeCombinations.{Insufficient, Rejected, Sufficient, ValidationResult}

/**
  * Created by Jan on 16.06.2017.
  */
object SingleBucket {
  private def weight(s: Seq[Query]): Long = s.flatMap(_.fragments).distinct.aggregate(0l)(_ + _.size, _ + _)

  private def countBasedDepthFun(depth: Int)(q: Seq[Query]): ValidationResult =
    if (q.length >= depth) Sufficient else Insufficient

  private def shareBasedDepthFun(barrier: Double, shares: Map[Query, Double])(s: Seq[Query]): ValidationResult =
    if (s.tail.flatMap(q => shares.get(q)).sum >= barrier) Rejected
    else if (s.flatMap(r => shares.get(r)).sum >= barrier) Sufficient
    else Insufficient

  def distributeBySize(queries: Seq[Query], depth: Int): Seq[(Seq[Query], Long)] = {
    val res = TreeCombinations(queries, countBasedDepthFun(depth))
    res.linearize.map(s => (s, weight(s)))
  }

  def distributeByShare(queries: Seq[Query], shares: Map[Query, Double], barrier: Double): Seq[(Seq[Query], Long)] = {
    val res = TreeCombinations(queries, shareBasedDepthFun(barrier, shares))
    res.linearize.map(s => (s, weight(s)))
  }
}
