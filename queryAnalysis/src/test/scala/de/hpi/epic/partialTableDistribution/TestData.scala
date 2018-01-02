package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.data.{Column, Query}

/**
  * Created by Jan on 09.07.2017.
  */
object TestData {
  def A = Column("A", 4, 10)
  def B = Column("B", 10, 10)
  def C = Column("C", 4, 10)
  def D = Column("D", 15, 10)

  val input = IndexedSeq(
    Query("1", Seq(A, B)),
    Query("2", Seq(B, D)),
    Query("3", Seq(B, C)),
    Query("4", Seq(A, B, C))
  )
}