package de.hpi.epic.partialTableDistribution.sql

/**
  * Created by Jan on 27.05.2017.
  */
case class Order(exp: Expression, ordering: SQLOrdering = ASC) extends SQLElement

trait SQLOrdering
case object ASC extends SQLOrdering
case object DESC extends SQLOrdering
