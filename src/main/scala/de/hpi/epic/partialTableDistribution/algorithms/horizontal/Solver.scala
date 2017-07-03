package de.hpi.epic.partialTableDistribution.algorithms.horizontal

import de.hpi.epic.partialTableDistribution.sql._

case object True extends Condition
case object False extends Condition

/**
  * Created by Jan on 26.06.2017.
  */
object Solver {
  def normalizeNegation(condition: Condition): Condition = condition match {
    case ConjunctionCondition(left, right) => DisjunctionCondition(NegationCondition(left), NegationCondition(right))
    case DisjunctionCondition(left, right) => ConjunctionCondition(NegationCondition(left), NegationCondition(right))
    case NegationCondition(condition) => condition
    case Exists(query, negation) => Exists(query, !negation)
    case Comparison(left, comparator, qualifier, right) =>
      val negatedComp = comparator match {
        case Equal => NotEqual
        case NotEqual => Equal
        case GreaterThan => LessEqual
        case LessThan => GreaterEqual
        case GreaterEqual => LessThan
        case LessEqual => GreaterThan
      }
      Comparison(left, negatedComp, qualifier, right)
    case Like(left, right, negation) => Like(left, right, !negation)
    case Between(left, start, end, negation) => Between(left, start, end, !negation)
    case In(left, right, negation) => In(left, right, !negation)
  }

  def solve(c: Condition, params: Map[String, Value]): Condition = {
    c match {
      case ConjunctionCondition(left, right) => throw new NotImplementedError("and")
      case DisjunctionCondition(left, right) => throw new NotImplementedError("or")
      case NegationCondition(condition) =>
        val negatedCondition = normalizeNegation(condition)
        solve(negatedCondition, params)
      case p: Predicate => solvePredicate(p, params)
    }
  }

  private def solvePredicate(p: Predicate, params: Map[String, Value]): Condition = {
    p match {
      case Exists(query, negation) => throw new NotImplementedError("exists")
      case c: Comparison => solveComparison(c, params)
      case Like(left, right, negation) => throw new NotImplementedError("like")
      case Between(left, start, end, negation) => throw new NotImplementedError("between")
      case In(left, right, negation) => throw new NotImplementedError("in")
      case _ => throw new NotImplementedError(s"$p")
    }
  }

  private def solveComparison(c: Comparison, params: Map[String, Value]): Condition = c.comparator match {
    case Equal =>
      val left = reduceExpression(c.left, params)
      //TODO: fix this
      val right = if (c.right.isLeft) reduceExpression(c.right.left.get.head, params) else ???
      (left, right) match {
        case (c1: Column, c2: Column) => True
        case (v1: Value, v2: Value) => ???
        case _ => Comparison(left, c.comparator, c.qualifier, Left(Seq(right)))
      }
    case NotEqual => ???
    case GreaterThan => ???
    case LessThan => ???
    case GreaterEqual => ???
    case LessEqual => ???
    case _ => throw new NotImplementedError(s"$c")
  }

  private def reduceExpression(e: Expression, params: Map[String, Value]): Expression = e match {
    case c: Column => c
    case OperationExpression(left, operator, right) =>
      val e1 = reduceExpression(left, params)
      val e2 = reduceExpression(right, params)
      (e1, e2) match {
        case (v1 : Value, v2 : Value) => doOperation(v1, operator, v2)
        case _ => OperationExpression(e1, operator, e2)
      }
    case c: Constant => c match {
      case SQLVariable(name) => params.getOrElse(name, throw new Error("Not found"))
      case _ => SingleValue(c)
    }
    case _ => ???
  }

  private def doOperation(l: Value, operator: String, r: Value): Value = operator match {
    case "+" => l + r
  }
}
