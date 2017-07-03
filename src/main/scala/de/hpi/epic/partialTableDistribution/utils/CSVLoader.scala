package de.hpi.epic.partialTableDistribution.utils

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by Jan on 16.06.2017.
  */
object CSVLoader {
  def load(filename: String, separator: Char = ';'): Seq[Seq[String]] = {
    val b = ListBuffer.empty[Seq[String]]
    for (line <- Source.fromFile(filename).getLines) {
      b += line.split(separator)
    }
    b.toSeq
  }
}
