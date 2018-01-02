package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.{BinPacking, PCBinPacking, ParallelBinPacking}
import de.hpi.epic.partialTableDistribution.data.{Column, Query}
import org.scalatest.WordSpec

/**
  * Created by Jan on 01.06.2017.
  */
object TestData2 {
  def A = Column("A", 4, 10)
  def B = Column("B", 10, 10)
  def C = Column("C", 4, 10)
  def D = Column("D", 15, 10)

  val Q1 = Query("1", Seq(A, B))
  val Q2 = Query("2", Seq(A, C))
  val Q3 = Query("3", Seq(D, B))
  val Q4 = Query("4", Seq(C, B, D))
}

class BinPackingSpec extends WordSpec {
  "the algorithm" should {
    "work for an even distribution" in {
      import TestData2._
      val res = BinPacking.apply(Seq(Q1, Q2, Q3, Q4), 2)
      res.foreach(d => println(
        d._3 + ":" +
          d._1.values.map(_.map(_.name).mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          d._2.mkString("[", ", ", "]")
      ))
    }

    /*"work for an uneven distribution" in {
      import TestData2._
      val res = BinPacking.apply(Seq(Q1, Q2, Q3, Q4), 3)
      res.foreach(d => println(
        d._3 + ":" +
          d._1.values.map(_.map(_.id).mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          d._2.mkString("[", ", ", "]")
      ))
    }*/
  }

  "the parallel algorithm" should {
    "work for an even distribution" in {
      import TestData2._
      val res = PCBinPacking.execute(Seq(Q1, Q2, Q3, Q4), 2)
      println(res.map(_._2.map(_.name).mkString("[", ", ", "]")).mkString("\n"))
    }

    /*"work for an uneven distribution" in {
      import TestData2._
      val res = BinPacking.apply(Seq(Q1, Q2, Q3, Q4), 3)
      res.foreach(d => println(
        d._3 + ":" +
          d._1.values.map(_.map(_.id).mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          d._2.mkString("[", ", ", "]")
      ))
    }*/
  }
}
