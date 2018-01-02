package de.hpi.epic.partialTableDistribution.sql

/**
  * Created by Jan on 26.05.2017.
  */

case class AliasCorrelation(correlation: Correlation, alias: Option[String] = None) extends SQLElement

trait Correlation extends SQLElement

case class Table(name: String, schema: Option[String] = None) extends Correlation
case class Join(left: AliasCorrelation, right: AliasCorrelation, natural: Boolean = false, joinType: JoinType = INNER, condition: Option[Condition] = None) extends Correlation

trait JoinType
case object INNER extends JoinType
case object LEFT_OUTER extends JoinType
case object RIGHT_OUTER extends JoinType
case object FULL_OUTER extends JoinType
case object UNION extends JoinType