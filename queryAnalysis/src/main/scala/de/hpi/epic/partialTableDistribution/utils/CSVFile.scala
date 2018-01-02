package de.hpi.epic.partialTableDistribution.utils

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable
import mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

/**
  * Created by Jan on 13.10.2017.
  */

class CSVFile(private val data: Array[Array[String]], private val header: Array[String]) {
  lazy val rowCount: Int = data.length
  lazy val columnCount: Int = data.headOption.map(_.length).getOrElse(0)
  def row(i: Int): CSVRow = new CSVRow(data(i), header)
  def filter(f: CSVRow => Boolean): Array[CSVRow] = data.map(r => new CSVRow(r, header)).filter(f)
  def find(f: CSVRow => Boolean): Option[CSVRow] = data.map(r => new CSVRow(r, header)).find(f)
  def foreach(f: CSVRow => Unit): Unit = data.foreach(a => f(new CSVRow(a, header)))
  def exists(f: CSVRow => Boolean): Boolean = data.exists(r => f(new CSVRow(r, header)))

  def save(path: String, delimiter: String = ";"): Unit = {
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(header.mkString("", delimiter, System.lineSeparator()))
    data.foreach(row => bw.write(row.mkString("", delimiter, System.lineSeparator())))
    bw.close()
  }

  def copy: CSVFile = {
    val data_copy = Array.ofDim[Array[String]](data.length)
    for(x <- 0 until data.length) {
      data_copy(x) = Array.ofDim[String](data(x).length)
      for (y <- 0 until data(x).length) {
        data_copy(x)(y) = data(x)(y)
      }
    }
    new CSVFile(data_copy, header)
  }
}

class CSVRow(private val data: Array[String], private val header: Array[String]) {
  def getString(index: Int): String = data(index)
  def getString(column: String): String = data(header.indexOf(column))
  def setString(index: Int, value: String): Unit = data.update(index, value)
  def setString(column: String, value: String) = data.update(header.indexOf(column), value)

  def getInt(index: Int): Int = data(index).toInt
  def getInt(column: String): Int = data(header.indexOf(column)).toInt
  def setInt(index: Int, value: Int): Unit = data.update(index, value.toString)
  def setInt(column: String, value: Int) = data.update(header.indexOf(column), value.toString)

  def getBigDecimal(index: Int): BigDecimal = BigDecimal(data(index))
  def getBigDecimal(column: String): BigDecimal = BigDecimal(data(header.indexOf(column)))
  def setBigDecimal(index: Int, value: BigDecimal): Unit = data.update(index, value.toString)
  def setBigDecimal(column: String, value: BigDecimal) = data.update(header.indexOf(column), value.toString)
}

object CSVFile {
  def apply(filename: String, delimiter: String = ";"): CSVFile = {
    val a = ArrayBuffer.empty[Array[String]]
    for (line <- Source.fromFile(filename).getLines) {
      a += line.split(delimiter)
    }
    new CSVFile(a.tail.toArray, a.head)
  }
}