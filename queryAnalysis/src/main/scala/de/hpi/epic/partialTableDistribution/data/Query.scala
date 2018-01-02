package de.hpi.epic.partialTableDistribution.data

/**
  * Created by Jan on 16.12.2017.
  */
case class Query(name: String,
                 fragments: Seq[Fragment],
                 load: BigDecimal = BigDecimal(0),
                 classification: Classification = ReadQuery) extends Application
