package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.utils.TreeCombinations.{Insufficient, Rejected, Sufficient, ValidationResult}
import de.hpi.epic.partialTableDistribution.utils.{Column, Query, SQLInteger, SQLVarChar, TreeCombinations}
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.MustMatchers._

object TestData {
  def A = Column("A", SQLInteger, None, 10)
  def B = Column("B", SQLVarChar, Some(10), 10)
  def C = Column("C", SQLInteger, None, 10)
  def D = Column("D", SQLVarChar, Some(15), 10)

  val input = IndexedSeq(
    Query("1", Set(A, B)),
    Query("2", Set(B, D)),
    Query("3", Set(B, C)),
    Query("4", Set(A, B, C))
  )
}

class CombinationsSpec extends WordSpec with Matchers {
  import TestData._
  "The Combinator" should {
    "return 2 queries for 4 passed queries when 50% selected" in {
      val res = TreeCombinations[Query](input, s => if (s.length >= 2) TreeCombinations.Sufficient else TreeCombinations.Insufficient )
      val linearized = res.linearize
      linearized shouldBe Seq(
        Seq(input(0), input(1)),
        Seq(input(0), input(2)),
        Seq(input(0), input(3)),
        Seq(input(1), input(2)),
        Seq(input(1), input(3)),
        Seq(input(2), input(3))
      )
    }

    "return different number of queries, so that the accumulated percentage is higher than needed" in {
      lazy val shares = Map[Query, Double](
        input(0) -> 0.3,
        input(1) -> 0.1,
        input(2) -> 0.2,
        input(3) -> 0.4
      )

      def depthFun(barrier: Double)(s: Seq[Query]): ValidationResult =
        if (s.tail.flatMap(q => shares.get(q)).sum >= barrier) Rejected
        else if (s.flatMap(r => shares.get(r)).sum >= barrier) Sufficient
        else Insufficient

      val res = TreeCombinations(input, depthFun(0.25))
      res.linearize shouldBe Seq(
        Seq(input(0)),
        Seq(input(1), input(2)),
        Seq(input(3))
      )
    }
  }
}
