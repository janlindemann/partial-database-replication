package de.hpi.epic.partialTableDistribution.algorithms.clustering

import de.hpi.epic.partialTableDistribution.data.Query

/**
  * Created by Jan on 17.07.2017.
  */
object OverlappingQueries {
  def included(queries: Seq[Query]): Seq[(Query, Query)] = {
    queries.flatMap(q => queries.filter(o => o != q && q.fragments.toSet.subsetOf(o.fragments.toSet)).map((q, _)))
  }
}
