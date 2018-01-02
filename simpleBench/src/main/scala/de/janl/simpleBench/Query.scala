package de.janl.simpleBench

import java.io.File
import java.sql.{Connection, SQLTimeoutException}

import org.postgresql.util.PSQLException

import scala.io.Source

/**
  * Created by Jan on 01.11.2017.
  */
trait Query {
  def id: String
  protected def sqlString: String
  def execute(connection: Connection): Long = {
    print(id)
    val start = System.nanoTime()
    val stmt = connection.createStatement()
    stmt.setQueryTimeout(60)
    try {
      val resultSet = stmt.executeQuery(sqlString)
      val md = resultSet.getMetaData
      val resultColumnCount = md.getColumnCount
      while (resultSet.next()) {
        val row = for (i <- 1 to resultColumnCount) yield resultSet.getObject(i)
        assert(row.length == resultColumnCount)
      }
    } catch {
      case e: PSQLException => print(" timeout")
    }
    val res = System.nanoTime() - start
    println(s": ${res/1000/1000}ms")
    res
  }
}

object Query {
  case class QueryFromFile(file: File) extends Query {
    val id = file.getName.split('.').head
    private val bufferedSource = Source.fromFile(file)
    val sqlString = bufferedSource.getLines.mkString(System.lineSeparator())
    bufferedSource.close()
  }
  def load(path: String): Seq[Query] = {
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).filter(_.getName.endsWith(".sql")).map(QueryFromFile)
    } else {
      Seq.empty[Query]
    }
  }
}
