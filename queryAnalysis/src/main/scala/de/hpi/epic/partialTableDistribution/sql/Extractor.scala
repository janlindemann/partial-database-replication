package de.hpi.epic.partialTableDistribution.sql

/**
  * Created by Jan on 27.05.2017.
  */
object Extractor {
  def fold[B](start: B, op: (B, SQLElement) => B, input: SQLElement): B = {
    val res = op(start, input)
    input match {
      case q: Query => foldQuery(res, op, q)
      case e: Expression => foldExpression(res, op, e)
      case c: Correlation => foldCorrelation(res, op, c)
      case c: Condition => foldCondition(res, op, c)
      case p: Projection => foldProjection(res, op, p)
      case Order(exp, _) => fold(res, op, exp)
      case AliasCorrelation(correlation, _) => fold(res, op, correlation)
    }
  }

  private def foldProjection[B](start: B, op: (B, SQLElement) => B, input: Projection): B = {
    input match {
      case AliasExpression(expression, _) => fold(start, op, expression)
      case StarExpression => start
    }
  }

  private def foldPredicate[B](start: B, op: (B, SQLElement) => B, input: Predicate): B = {
    input match {
      case Comparison(left, _, _, right) =>
        val res = fold(start, op, left)
        right.fold(_.foldLeft(res)((p,c) => fold(p,op,c)), fold(res, op, _))
      case Like(left, right, _) =>
        val res = fold(start, op, left)
        fold(res, op, right)
      case Exists(query, _) => fold(start, op, query)
      case Between(left, begin, end, _) =>
        val res1 = fold(start, op, left)
        val res2 = fold(res1, op, begin)
        fold(res2, op, end)
      case In(left, right, _) =>
        val res = fold(start, op, left)
        right.fold(_.foldLeft(res)((p,c) => fold(p, op, c)), fold(res, op, _))
    }
  }

  private def foldCondition[B](start: B, op: (B, SQLElement) => B, input: Condition): B = {
    input match {
      case ConjunctionCondition(left, right) =>
        val res1 = fold(start, op, left)
        fold(res1, op, right)
      case DisjunctionCondition(left, right) =>
        val res1 = fold(start, op, left)
        fold(res1, op, right)
      case NegationCondition(condition) =>
        fold(start, op, condition)
      case p:Predicate => foldPredicate(start, op, p)
    }
  }

  private def foldCorrelation[B](start: B, op: (B, SQLElement) => B, input: Correlation): B = {
    input match {
      case Table(_, _) => start
      case Join(left, right, _, _, condition) =>
        val res1 = fold(start, op, left)
        val res2 = fold(res1, op, right)
        condition.map(fold(res2, op, _)).getOrElse(res2)
    }
  }

  private def foldAggregationExpression[B](start: B, op: (B, SQLElement) => B, input: AggregationExpression): B = {
    input match {
      case Sum(expression, _) => fold(start, op, expression)
      case Avg(expression, _) => fold(start, op, expression)
      case Min(expression, _) => fold(start, op, expression)
      case Max(expression, _) => fold(start, op, expression)
      case Count(expression, _) => fold(start, op, expression)
      case CountStar => start
    }
  }

  private def foldSQLFunction[B](start: B, op: (B, SQLElement) => B, input: SQLFunction): B = {
    input match {
      case DateFunction(expression) => fold(start, op, expression)
      case IntervalFunction(_, _) => start
      case ExtractFunction(_, expression) => fold(start, op, expression)
      case SubstringFunction(expression, _, _) => fold(start, op, expression)
    }
  }

  private def foldCaseExpression[B](start: B, op: (B, SQLElement) => B, input: CaseExpression): B = {
    input match {
      case SearchCaseExpression(cases, alternative) =>
        val res = cases.foldLeft(start)((p,c) => {
          val res = fold(p,op,c.condition)
          fold(res, op, c.expression)
        })
        alternative.map(fold(res, op, _)).getOrElse(res)
    }
  }

  private def foldExpression[B](start: B, op: (B, SQLElement) => B, input: Expression): B = {
    input match {
      case Column(_, _) => start
      case OperationExpression(left, _, right) =>
        val res1 = fold(start, op, left)
        fold(res1, op, right)
      case exp: AggregationExpression => foldAggregationExpression(start, op, exp)
      case exp: SQLFunction => foldSQLFunction(start, op, exp)
      case exp: Constant => start
      case c: CaseExpression => foldCaseExpression(start, op, c)
    }
  }

  private def foldQuery[B](start: B, op: (B, SQLElement) => B, input: Query): B = {
    input match {
      case Select(projections, correlations, condition, grouping, having, ordering) =>
        val res1 = projections.foldLeft(start)((p,c) => fold(p,op,c))
        val res2 = correlations.foldLeft(res1)((p,c) => fold(p,op,c))
        val res3 = condition.map(fold(res2, op, _)).getOrElse(res2)
        val res4 = grouping.map(_.foldLeft(res3)((p,c) => fold(p,op,c))).getOrElse(res3)
        val res5 = having.map(fold(res4, op, _)).getOrElse(res4)
        val res6 = ordering.map(_.foldLeft(res5)((p,c) => fold(p,op,c))).getOrElse(res5)
        res6
    }
  }
}
