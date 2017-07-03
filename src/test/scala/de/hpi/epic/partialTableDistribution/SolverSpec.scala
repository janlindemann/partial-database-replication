package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.algorithms.horizontal
import de.hpi.epic.partialTableDistribution.algorithms.horizontal._
import de.hpi.epic.partialTableDistribution.sql._
import org.scalatest.{MustMatchers, WordSpec}

/**
  * Created by Jan on 26.06.2017.
  */
class SolverSpec extends WordSpec with MustMatchers {
  "The solver" should {
    "return true for comparisons of columns" in {
      val c1 = Comparison(Column("A"), Equal, None, Left(Seq(Column("A"))))
      Solver.solve(c1, Map.empty[String, Value]) mustBe True

      val c2 = Comparison(Column("A"), Equal, None, Left(Seq(Column("B"))))
      Solver.solve(c2, Map.empty[String, Value]) mustBe True
    }

    "minimize an operation" in {
      val c1 = Comparison(Column("A"), Equal, None, Left(Seq(OperationExpression(SQLInteger(4), "+", SQLInteger(3)))))
      Solver.solve(c1, Map.empty[String, Value]) mustEqual Comparison(Column("A"), Equal, None, Left(Seq(SingleValue(SQLInteger(7)))))
    }

    "minimize a parameterized operation" in {
      val c1 = Comparison(Column("A"), Equal, None, Left(Seq(OperationExpression(SQLVariable("INT"), "+", SQLInteger(3)))))

      Solver.solve(c1, Map[String, Value]("INT" -> SingleValue(SQLInteger(4)))) mustEqual Comparison(Column("A"), Equal, None, Left(Seq(SingleValue(SQLInteger(7)))))
      Solver.solve(c1, Map[String, Value]("INT" -> ValueList(List(SQLInteger(4), SQLInteger(5))))) mustEqual Comparison(Column("A"), Equal, None, Left(Seq(ValueList(List(SQLInteger(7), SQLInteger(8))))))
    }

    "minimize an operation of list and UpperBoundInclusive" in {
      val c = Comparison(Column("A"), Equal, None, Left(Seq(OperationExpression(SQLVariable("A"), "+", SQLVariable("B")))))

      Solver.solve(
        c,
        Map[String, Value](
          "A" -> ValueList(List(SQLInteger(4), SQLInteger(5))),
          "B" -> UpperBoundInclusive(SQLInteger(10))
        )
      )
        .mustEqual(
          Comparison(
            Column("A"),
            Equal,
            None,
            Left(
              Seq(UpperBoundInclusive(SQLInteger(15)))
            )
          )
        )
    }
  }

}
