package de.hpi.epic.partialTableDistribution.sql

/**
  * Created by Jan on 25.05.2017.
  */

trait Projection extends SQLElement
case class AliasExpression(expression: Expression, alias: Option[String] = None) extends Projection
case object StarExpression extends Projection

trait Expression extends SQLElement

case class Column(name: String, correlation: Option[String] = None) extends Expression

trait SQLFunction extends Expression
case class DateFunction(expression: Expression) extends SQLFunction
case class IntervalFunction(value: String, qualifier: IntervalQualifier) extends SQLFunction
case class ExtractFunction(datetimeField: DatetimeField, expression: Expression) extends SQLFunction
case class SubstringFunction(expression: Expression, pos: Int, length: Option[Int]) extends SQLFunction

case class IntervalQualifier(datetimeField: DatetimeField, precision: Option[Int])
trait DatetimeField extends SQLElement
case object YEAR extends DatetimeField
case object MONTH extends DatetimeField
case object DAY extends DatetimeField
case object HOUR extends DatetimeField
case object MINUTE extends DatetimeField

case class OperationExpression(left: Expression, operator: String, right: Expression) extends Expression

trait AggregationExpression extends Expression
case class Sum(expression: Expression, distinct: Boolean = false) extends AggregationExpression
case class Avg(expression: Expression, distinct: Boolean = false) extends AggregationExpression
case class Min(expression: Expression, distinct: Boolean = false) extends AggregationExpression
case class Max(expression: Expression, distinct: Boolean = false) extends AggregationExpression
case class Count(expression: Expression, distinct: Boolean = false) extends AggregationExpression
case object CountStar extends AggregationExpression

trait CaseExpression extends Expression
case class SearchCaseExpression(cases: Seq[SearchCase], alternative: Option[Expression]) extends CaseExpression
case class SearchCase(condition: Condition, expression: Expression) extends SQLElement