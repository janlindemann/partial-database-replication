package de.hpi.epic.partialTableDistribution

/**
  * Created by Jan on 29.05.2017.
  */
package object utils {
  class ByteConverter(size: Long) {
    def KB: String = s"${"%.2f".format(size / 1024f)} KB"
    def MB: String = s"${"%.2f".format(size / 1024f / 1024)} MB"
    def GB: String = s"${"%.2f".format(size / 1024f / 1024f / 1024f)} GB"
  }

  class PercentageConverter(value: Double) {
    def percentage : String = s"${"%.2f".format(value * 100d)}%"
  }

  implicit def toByteConverter(size: Long): ByteConverter = new ByteConverter(size)
  implicit def toPercentageConverter(percentage: Double): PercentageConverter = new PercentageConverter(percentage)
}
