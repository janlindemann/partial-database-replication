package de.hpi.epic.partialTableDistribution.utils

trait Tree[T] {
  def traverse(function: (T, Int) => Unit, level: Int = 0): Unit
  def linearize: Seq[Seq[T]]
}

sealed trait CanBeChild

trait HasValue[T] {
  def value: T
}

case class Root[T](children: Seq[Tree[T] with CanBeChild]) extends Tree[T] {
  override def traverse(function: (T, Int) => Unit, level: Int): Unit =
    children.foreach(_.traverse(function, level + 1))
  override def linearize: Seq[Seq[T]] =
    children.flatMap(_.linearize)
}

case class Node[T](children: Seq[Tree[T] with CanBeChild], value: T) extends Tree[T] with CanBeChild with HasValue[T]{
  def traverse(function: (T, Int) => Unit, level: Int = 0): Unit = {
    function(this.value, level + 1)
    children.foreach(_.traverse(function, level + 1))
  }
  def linearize: Seq[Seq[T]] = children.flatMap(_.linearize).map(value +: _)
}

case class Leaf[T](value: T) extends Tree[T] with CanBeChild with HasValue[T] {
  def traverse(function: (T, Int) => Unit, level: Int = 0) = function(this.value, level + 1)
  def linearize: Seq[Seq[T]] = Seq(Seq(value))
}