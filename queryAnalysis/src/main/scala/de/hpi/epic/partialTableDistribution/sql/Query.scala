package de.hpi.epic.partialTableDistribution.sql

/**
  * Created by Jan on 25.05.2017.
  */
trait Query extends Correlation

case class Select(
                   projections: Seq[Projection],
                   correlations: Seq[AliasCorrelation],
                   condition: Option[Condition] = None,
                   grouping: Option[Seq[Expression]] = None,
                   having: Option[Condition] = None,
                   ordering: Option[Seq[Order]] = None
                 ) extends Query
