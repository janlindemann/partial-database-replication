package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.data.{Query, Table}

/**
  * Created by Jan on 16.12.2017.
  */
trait Workload {
  def tables: Seq[Table]
  def queries: Seq[Query]
}
