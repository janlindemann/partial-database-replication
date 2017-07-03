package de.hpi.epic.partialTableDistribution.algorithms.horizontal

import de.hpi.epic.partialTableDistribution.sql.{Constant, Expression}

/**
  * Created by Jan on 26.06.2017.
  */
trait Value extends Expression {
  def +(r: Value): Value
}

case class SingleValue(value: Constant) extends Value {
  def +(other: Value): Value = other match {
    case SingleValue(v) => SingleValue(value + v)
    case _ => other + this
  }
}

case class ValueList(values: List[Constant]) extends Value{
  def +(other: Value): Value = other match {
    case SingleValue(v) => ValueList(values.map(_  + v))
    case ValueList(v) => ValueList(values.flatMap(e => v.map(_ + e)))
    case _ => other + this
  }
}

case class UpperBoundInclusive(value: Constant) extends Value {
  def +(other: Value): Value = other match {
    case SingleValue(v) => UpperBoundInclusive(value + v)
    case ValueList(v) => UpperBoundInclusive(v.max + value)
    case UpperBoundInclusive(v) => UpperBoundInclusive(v + value)
    case _ => other + this
  }
}

case class UpperBoundExclusive(value: Constant) extends Value {
  def +(other: Value): Value = other match {
    case SingleValue(v) => UpperBoundExclusive(value + v)
    case ValueList(v) => UpperBoundExclusive(v.max + value)
    case UpperBoundInclusive(v) => UpperBoundExclusive(v + value)
    case UpperBoundExclusive(v) => UpperBoundExclusive(v + value)
    case _ => other + this
  }
}
/*
case class LowerBoundInclusive[T](value: T) extends Value[T]
case class LowerBoundExclusive[T](value: T) extends Value[T]
case class InclusiveRange[T](lowerBound: T, upperBound: T) extends Value[T]
case class ExclusiveRange[T](lowerBound: T, upperBound: T) extends Value[T]*/
