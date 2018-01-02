package de.hpi.epic.partialTableDistribution

/**
  * Created by Jan on 29.05.2017.
  */
package object utils {
  class ByteConverter(size: Long) {
    def KB: String = s"${"%.2f".format(size / 1000f)} KB"
    def MB: String = s"${"%.2f".format(size / 1000f / 1000f)} MB"
    def GB: String = s"${"%.2f".format(size / 1000f / 1000f / 1000f)} GB"
  }

  class PercentageConverter(value: Double) {
    def percentage : String = s"${"%.2f".format(value * 100d)}%"
  }

  implicit def toByteConverter(size: Long): ByteConverter = new ByteConverter(size)
  implicit def toPercentageConverter(percentage: Double): PercentageConverter = new PercentageConverter(percentage)

  def time[A](a: => A): A = {
    val now = System.nanoTime
    val result = a
    val micros = (System.nanoTime - now) / 1000
    println("%d microseconds".format(micros))
    result
  }

  def timeAnd[A](a: => A): (Long, A) = {
    val now = System.nanoTime()
    val result = a
    val runtime = (System.nanoTime() - now) / 1000
    (runtime, result)
  }

  def timeOnly[A](a: => A): Long = {
    val now = System.nanoTime()
    a
    val runtime = (System.nanoTime() - now) / 1000
    runtime
  }
}
