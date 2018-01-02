package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.heuristics.DivideAndConquerHeuristic
import de.hpi.epic.partialTableDistribution.data._
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by Jan on 29.09.2017.
  */
class DivideAndConquerSpec extends WordSpec with Matchers {
  case class Backend(name: String, capacity: BigDecimal) extends Side

  val tables = Map(
    "A" -> Table("A", Seq(Column("A1", 3, 7))),
    "B" -> Table("B", Seq(Column("B1", 1, 11))),
    "C" -> Table("C", Seq(Column("C1", 1, 11))))
  val classifications: Seq[Query] = Seq(
    Query("Q1", Seq(tables("A")), BigDecimal(0.24d)),
    Query("Q2", Seq(tables("B")), BigDecimal(0.15d)),
    Query("Q3", Seq(tables("C")), BigDecimal(0.2d)),
    Query("Q4", Seq(tables("A"), tables("B")), BigDecimal(0.16d)),
    Query("Q5", Seq(tables("B"), tables("C")), BigDecimal(0.25d))
  )

  "The Heuristic" should {
    "return the right distribution for power of two" in {
      val backends: Seq[Backend] = (1 to 4) map (i => Backend(s"B$i", BigDecimal(1) / BigDecimal(4)))
      val h = new DivideAndConquerHeuristic
      val res = h.distribute(classifications, backends)
      res.foreach(t => println(s"${t._1.name}: ${t._2.map(_.name).mkString(", ")} (${t._2.map(_.size).sum})"))
    }

    "return a heuristic for numbers different from power of two" in {
      val backends: Seq[Backend] = (1 to 3) map (i => Backend(s"B$i", BigDecimal(1) / BigDecimal(3)))
      val h = new DivideAndConquerHeuristic
      val res = h.distribute(classifications, backends)
      res.foreach(t => println(s"${t._1.name}: ${t._2.map(_.name).mkString(", ")} (${t._2.map(_.size).sum})"))
    }
  }
}
