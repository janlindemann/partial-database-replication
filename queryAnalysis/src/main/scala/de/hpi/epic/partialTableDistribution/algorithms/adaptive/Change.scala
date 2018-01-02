package de.hpi.epic.partialTableDistribution.algorithms.adaptive

import de.hpi.epic.partialTableDistribution.data.Application

/**
  * Created by Jan on 08.11.2017.
  */
trait Change {
  def loadDiff: BigDecimal
  def isAddition: Boolean
  def isRemoval: Boolean
  def isChange: Boolean
}
case class NewApplication(newApp: Application) extends Change {
  override def loadDiff: BigDecimal = newApp.load

  override def isAddition: Boolean = true
  override def isChange: Boolean = false
  override def isRemoval: Boolean = false
}
case class RemovedApplication(oldApp: Application) extends Change {
  override def loadDiff: BigDecimal = oldApp.load * -1

  override def isAddition: Boolean = false
  override def isChange: Boolean = false
  override def isRemoval: Boolean = true
}
case class ChangedWeight(newApp: Application, oldApp: Application, diff: BigDecimal) extends Change {
  override def loadDiff: BigDecimal = diff

  override def isAddition: Boolean = false
  override def isChange: Boolean = true
  override def isRemoval: Boolean = false
}
