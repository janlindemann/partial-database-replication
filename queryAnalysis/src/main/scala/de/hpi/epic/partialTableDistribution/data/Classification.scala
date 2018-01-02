package de.hpi.epic.partialTableDistribution.data

/**
  * Created by Jan on 13.10.2017.
  */
trait Classification
case object ReadQuery extends Classification
case object UpdateQuery extends Classification
