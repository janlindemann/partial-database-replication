package de.janl.simpleBench.tpch.queries

import java.sql.{Connection, Date}

import de.janl.simpleBench.Query

import scala.io.Source

/**
  * Created by Jan on 01.11.2017.
  */
class RefreshFunction2(val orders: String) extends Query {
  val id: String = "RF2"
  private val orderskeys = {
    val bufferedSource = Source.fromFile(orders)
    val res = bufferedSource.getLines.map(_.split('|').head).toList
    bufferedSource.close
    res
  }
  protected val sqlString: String = s"DELETE FROM orders where o_orderkey = ?"
  val lineitemsString = s"DELETE FROM lineitem where l_orderkey = ?"


  override def execute(connection: Connection): Long = {
    print(s"${id}_lineitem")
    val start = System.nanoTime()
    val stmt = connection.prepareStatement(lineitemsString)

    orderskeys.zipWithIndex.foreach(t => {
      if (t._2 % 100 == 0) println(t._2)
      stmt.setInt(1, t._1.toInt)
      if (stmt.executeUpdate() < 1) println("ERROR")
    })


    val res = System.nanoTime() - start
    println(s": ${res/1000/1000}ms")
    print(s"${id}_orders")
    val start2 = System.nanoTime()

    val orderStmt = connection.prepareStatement(sqlString)

    orderskeys.zipWithIndex.foreach(t => {
      if (t._2 % 100 == 0) println(t._2)
      orderStmt.setInt(1, t._1.toInt)
      if (orderStmt.executeUpdate() < 1) println("ERROR")
    })

    val res2 = System.nanoTime() - start2
    println(s": ${res2/1000/1000}ms")
    res + res2
  }
}

