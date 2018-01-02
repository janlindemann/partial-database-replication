package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.ShareBasedBinPacking
import de.hpi.epic.partialTableDistribution.data.{Fragment, Query}
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by Jan on 09.07.2017.
  */
class ShareBasedBinPackingSpec extends WordSpec with Matchers {
  def calculate_sizes(d: Map[Int, Seq[Query]]) =
    d.values.map(_.foldLeft(Set.empty[Fragment])(_ ++ _.fragments).foldLeft(0l)(_ + _.size)).toSeq

  import TestData._
  "return different number of queries, so that the accumulated percentage is higher than needed" in {
    lazy val shares = Map[Query, Double](
      input(0) -> 0.4,
      input(1) -> 0.1,
      input(2) -> 0.1,
      input(3) -> 0.4
    )

    val packing = new ShareBasedBinPacking(input.toList, shares)
    val res = packing.distributePart(3, 0.1d, (0, 14))
    res.foreach(d => {
      val sizes = calculate_sizes(d)
      println(
        sizes.sum.toString + ": " + d.map(_._2.map(_.name).mkString("[", ", ", "]")).mkString("[ ", ", ", " ]")
      )
    })
  }
}
