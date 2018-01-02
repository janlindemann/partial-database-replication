package de.hpi.epic.partialTableDistribution.sql

/**
  * Created by Jan on 27.05.2017.
  */
trait Condition extends SQLElement

case class ConjunctionCondition(left: Condition, right: Condition) extends Condition
case class DisjunctionCondition(left: Condition, right: Condition) extends Condition
case class NegationCondition(condition: Condition) extends Condition

trait Predicate extends Condition
case class Exists(query: Select, negation: Boolean = false) extends Predicate
case class Comparison(left: Expression, comparator: Comparator, qualifier: Option[ComparisonQualifier], right: Either[Seq[Expression], Select]) extends Predicate
case class Like(left: Expression, right: Expression, negation: Boolean = false) extends Predicate
case class Between(left: Expression, start: Expression, end: Expression, negation: Boolean = false) extends Predicate
case class In(left: Expression, right: Either[Seq[Expression], Select], negation: Boolean = false) extends Predicate

trait ComparisonQualifier
case object ALL extends ComparisonQualifier
case object ANY extends ComparisonQualifier

trait Comparator
case object Equal extends Comparator
case object NotEqual extends Comparator
case object GreaterThan extends Comparator
case object LessThan extends Comparator
case object GreaterEqual extends Comparator
case object LessEqual extends Comparator