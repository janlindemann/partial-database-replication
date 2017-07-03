package de.hpi.epic.partialTableDistribution.sql

/**
  * Created by Jan on 26.05.2017.
  */
trait Constant extends Expression  with Ordered[Constant]{
  def +(Value: Constant): Constant
}

case class SQLVariable(name: String) extends Constant {
  def +(other: Constant): Constant = throw new Error("You cannot execute operations directly on variables")
  def compare(that: Constant): Int = throw new Error("You cannot sort variables")
}

case class SQLInteger(value: Int) extends Constant {
  def +(other: Constant): Constant = other match {
    case SQLInteger(v) => SQLInteger(value + v)
    case _ => other + this
  }
  def compare(that: Constant): Int = that match {
    case SQLInteger(v) => value.compareTo(v)
    case _ => -1 * that.compare(this)
  }
}

case class SQLFloat(value: Float) extends Constant {
  def +(other: Constant): Constant = other match {
    case SQLInteger(v) => SQLFloat(value + v.toFloat)
    case SQLFloat(v) => SQLFloat(value + v)
    case _ => other + this
  }
  def compare(that: Constant): Int = that match {
    case SQLInteger(v) => value.compareTo(v)
    case SQLFloat(v) => value.compareTo(v)
    case _ => -1 * that.compare(this)
  }
}

case class SQLString(value: String) extends Constant {
  def +(other: Constant): Constant = other match {
    case SQLInteger(i) => SQLString(value + i.toString)
    case SQLFloat(f) => SQLString(value + f.toString)
    case SQLString(s) => SQLString(value + s)
    case _ => other + this
  }
  def compare(that: Constant): Int = that match {
    case SQLInteger(i) => value.compareTo(i.toString)
    case SQLFloat(f) => value.compareTo(f.toString)
    case SQLString(s) => value.compareTo(s)
    case _ => -1 * that.compare(this)
  }
}


