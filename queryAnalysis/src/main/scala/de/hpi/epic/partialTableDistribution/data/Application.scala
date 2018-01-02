package de.hpi.epic.partialTableDistribution.data

/**
  * Created by Jan on 27.09.2017.
  */
trait Application {
  def fragments: Seq[Fragment]
  def load: BigDecimal
  def classification: Classification
  def name: String
}