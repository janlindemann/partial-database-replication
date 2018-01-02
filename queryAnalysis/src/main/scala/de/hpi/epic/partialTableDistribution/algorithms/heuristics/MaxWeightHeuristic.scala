package de.hpi.epic.partialTableDistribution.algorithms.heuristics

import de.hpi.epic.partialTableDistribution.data.{Application, Fragment, Side}

import scala.annotation.tailrec

/**
  * Created by Jan on 26.07.2017.
  */
class MaxWeightHeuristic(similarityFn: (Seq[Application], Seq[Application]) => Double) extends Heuristic {
  override def distribute(applications: Seq[Application], sides: Seq[Side]): Map[Side, Seq[Fragment]] = {
    val startingClusters = applications.map(Seq(_)).toIndexedSeq
    val shares = startingClusters.map(s => (s, s.head.load)).toMap
    val res = distributeRecursive(startingClusters, shares, sides)
    res.mapValues(_.flatMap(_.fragments).distinct)
  }

  @tailrec
  private def distributeRecursive(input: IndexedSeq[Seq[Application]],
                                  remainingShare: Map[Seq[Application], BigDecimal],
                                  sides: Seq[Side],
                                  cluster: Map[Side, Seq[Application]] = Map.empty[Side, Seq[Application]]
                                 ): Map[Side, Seq[Application]]= {
    if (input.isEmpty || sides.isEmpty) {
      println()
      println(sides.length)
      println(cluster.size)
      println(input.length)
      cluster.foreach(c => println(s"${c._1.name}: ${c._2.map(_.name).mkString("[",",","]")}"))
      return cluster
    }

    val simMatrix = similarityMatrix(input, similarityFn)
    val (leftIndex, rightIndex) = strongestConnection(simMatrix)
    val (leftElements, rightElements) = (input(leftIndex), input(rightIndex))

    val leftShare = remainingShare(leftElements)
    val rightShare = remainingShare(rightElements)
    val newShare = leftShare + rightShare
    if (newShare > sides.head.capacity) {
      val (larger, smaller) = if (remainingShare(leftElements) < remainingShare(rightElements)) (rightElements, leftElements) else (leftElements, rightElements)
      val (finished, remaining, newShare) = recursiveAdd(larger, smaller, remainingShare, sides.head.capacity)
      val newInput = input.filterNot(s => s == leftElements || s == rightElements) :+ remaining
      val newCluster = cluster.updated(sides.head, finished)
      distributeRecursive(newInput, newShare, sides.tail, newCluster)
    } else if (newShare == sides.head.capacity) {
      val next = leftElements ++ rightElements
      val newInput = input.diff(Seq(leftElements, rightElements))
      val newCluster = cluster.updated(sides.head, next)
      distributeRecursive(newInput, remainingShare, sides.tail, newCluster)
    } else {
      val next = leftElements ++ rightElements
      val newInput = input.diff(Seq(leftElements, rightElements)) :+ next
      val updatedShare = remainingShare.updated(next, newShare)
      distributeRecursive(newInput, updatedShare, sides, cluster)
    }
  }

  private def similarityMatrix(input: IndexedSeq[Seq[Application]],
                               similarityFn: (Seq[Application], Seq[Application]) => Double
  ): IndexedSeq[IndexedSeq[Double]] = {
    input.map(q1 => input.map(q2 => 1 - similarityFn(q1, q2)))
  }

  private def strongestConnection(matrix: IndexedSeq[IndexedSeq[Double]]): (Int, Int) = {
    var min = Double.PositiveInfinity
    var index = (0,0)
    for (i <- matrix.indices;
         j <- i+1 until matrix.length) if (matrix(i)(j) < min) {
      min = matrix(i)(j)
      index = (i,j)
    }
    index
  }

  private def recursiveAdd(target: Seq[Application], source: Seq[Application],
                           remainingShare: Map[Seq[Application], BigDecimal], size: BigDecimal
                          ): (Seq[Application], Seq[Application],  Map[Seq[Application], BigDecimal]) = {
    if (remainingShare(target) == size)
      return (target, source, remainingShare)

    val next = source.minBy(app => similarityFn(target, Seq(app)))
    if (remainingShare(target) + remainingShare(Seq(next)) <= size) {
      val newTarget = target :+ next
      val newSource = source.filterNot(_ == next)
      val newShare = remainingShare
        .updated(newTarget, remainingShare(target) + remainingShare(Seq(next)))
        .updated(newSource, remainingShare(source) - remainingShare(Seq(next)))
      recursiveAdd(newTarget, newSource, newShare, size)
    } else {
      val newTarget = target :+ next
      val newShare = remainingShare
        .updated(newTarget, size)
        .updated(Seq(next), remainingShare(Seq(next)) - (size - remainingShare(target)))
        .updated(source, remainingShare(source) - (size - remainingShare(target)))
      recursiveAdd(newTarget, source, newShare, size)
    }
  }
}