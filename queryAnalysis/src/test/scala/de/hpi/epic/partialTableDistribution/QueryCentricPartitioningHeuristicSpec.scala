package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.heuristics.QueryCentricPartitioningHeuristic
import de.hpi.epic.partialTableDistribution.data.{Side, UpdateQuery}
import de.hpi.epic.partialTableDistribution.data.{Query, Table, Column}
import de.hpi.epic.partialTableDistribution.utils.Tabulator
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by Jan on 22.09.2017.
  */
class QueryCentricPartitioningHeuristicSpec  extends WordSpec with Matchers {
  case class Backend(name: String, capacity: BigDecimal) extends Side

  val tables = Map(
    "A" -> Table("A", Seq(Column("A1", 1, 11))),
    "B" -> Table("B", Seq(Column("B1", 1, 11))),
    "C" -> Table("C", Seq(Column("C1", 1, 11))))
  val classifications: Seq[Query] = Seq(
      Query("Q1", Seq(tables("A")), BigDecimal(0.24d)),
      Query("Q2", Seq(tables("B")), BigDecimal(0.2d)),
      Query("Q3", Seq(tables("C")), BigDecimal(0.2d)),
      Query("Q4", Seq(tables("A"), tables("B")), BigDecimal(0.16d)),
      Query("U1", Seq(tables("A")), BigDecimal(0.04d), UpdateQuery),
      Query("U2", Seq(tables("B")), BigDecimal(0.1d), UpdateQuery),
      Query("U3", Seq(tables("C")), BigDecimal(0.06d), UpdateQuery)
  )
  val backends: Seq[Backend] = IndexedSeq(Backend("B1", BigDecimal(0.3d)), Backend("B2", BigDecimal(0.3)), Backend("B3", BigDecimal(0.2)), Backend("B4", BigDecimal(0.2)))

  "The Heuristic" should {
    "return the right distribution" in {
      val heuristic = new QueryCentricPartitioningHeuristic()
      val result = heuristic.distribute(classifications, backends)
      println(result)
      val expected = Seq(0 -> Seq("A", "B"), 1 -> Seq("B", "C"), 2 -> Seq("A"), 3 -> Seq("C"))
      result shouldBe Map[QueryCentricPartitioningHeuristicSpec#Backend, Seq[Table]](
        expected.map(t => (backends(t._1), t._2.map(tables(_)))) : _*
      )
      heuristic.printLoadMatrix
    }
  }
}
