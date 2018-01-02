package de.hpi.epic.partialTableDistribution.utils

/**
  * Created by Jan on 24.05.2017.
  */
object TreeCombinations {
  trait ValidationResult
  case object Sufficient extends ValidationResult
  case object Insufficient extends ValidationResult
  case object Rejected extends ValidationResult

  type DepthFun[T] = Seq[T] => ValidationResult

  def apply[T](queries: Seq[T], depthFun: DepthFun[T]): Tree[T] = {
    Root[T](queries.tails.withFilter(_.nonEmpty).map(s => {
      buildTree[T](Seq.empty[T], s.head, s.tail, depthFun)
    }).toSeq.flatten)
  }

  def buildTree[T](previous: Seq[T], current: T, remaining: Seq[T], depthFun: DepthFun[T]): Option[Tree[T] with CanBeChild] = {
    val next = previous :+ current
    depthFun(next) match {
      case Rejected =>
        None
      case Sufficient =>
        Some(Leaf(current))
      case Insufficient =>
        val children = remaining.tails.withFilter(_.nonEmpty).map(s => {
          buildTree(next, s.head, s.tail, depthFun)
        }).toSeq.flatten
        if (children.isEmpty) {
          None
        } else {
          Some(Node(children, current))
        }
    }
  }
}
