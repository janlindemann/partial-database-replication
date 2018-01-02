package de.janl.simpleBench.tpch.queries

import java.sql.{Connection, Date}

import de.janl.simpleBench.Query

import scala.io.Source

/**
  * Created by Jan on 01.11.2017.
  */
class RefreshFunction1(val orders: String, val lineitems: String) extends Query {
  val id: String = "RF1"
  protected val sqlString: String = "INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
  val lineitemsString = "INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

  private val ordersData = {
    val bufferedSource = Source.fromFile(orders)
    val res = bufferedSource.getLines.map(_.split('|')).toList
    bufferedSource.close
    res
  }

  private val lineitemsData = {
    val bufferedSource = Source.fromFile(lineitems)
    val res = bufferedSource.getLines.map(_.split('|')).toList
    bufferedSource.close
    res
  }


  override def execute(connection: Connection): Long = {
    print(s"${id}_orders")
    val start = System.nanoTime()
    val oPreparedStatement = connection.prepareStatement(sqlString)
    ordersData.foreach(row => {
      oPreparedStatement.setInt(1, row(0).toInt)
      oPreparedStatement.setInt(2, row(1).toInt)
      oPreparedStatement.setString(3, row(2))
      oPreparedStatement.setFloat(4, row(3).toFloat)
      oPreparedStatement.setDate(5, Date.valueOf(row(4)))
      oPreparedStatement.setString(6, row(5))
      oPreparedStatement.setString(7, row(6))
      oPreparedStatement.setInt(8, row(7).toInt)
      oPreparedStatement.setString(9, row(8))

      if (oPreparedStatement.executeUpdate() != 1) println(s"Error ifor order ${row(0)}")
    })

    val res = System.nanoTime() - start
    println(s": ${res/1000/1000}ms")

    print(s"${id}_lineitem")
    val start2 = System.nanoTime()
    val lPreparedStatement = connection.prepareStatement(lineitemsString)
    lineitemsData.foreach(row => {
      lPreparedStatement.setInt(1, row(0).toInt)
      lPreparedStatement.setInt(2, row(1).toInt)
      lPreparedStatement.setInt(3, row(2).toInt)
      lPreparedStatement.setInt(4, row(3).toInt)
      lPreparedStatement.setFloat(5, row(4).toFloat)
      lPreparedStatement.setFloat(6, row(5).toFloat)
      lPreparedStatement.setFloat(7, row(6).toFloat)
      lPreparedStatement.setFloat(8, row(7).toFloat)
      lPreparedStatement.setString(9, row(8))
      lPreparedStatement.setString(10, row(9))
      lPreparedStatement.setDate(11, Date.valueOf(row(10)))
      lPreparedStatement.setDate(12, Date.valueOf(row(11)))
      lPreparedStatement.setDate(13, Date.valueOf(row(12)))
      lPreparedStatement.setString(14, row(13))
      lPreparedStatement.setString(15, row(14))
      lPreparedStatement.setString(16, row(15))

      val res = lPreparedStatement.executeUpdate()
      if (res != 1) println(s"ERROR lineitems ${row(0)}")
    })

    val res2 = System.nanoTime() - start2
    println(s": ${res2/1000/1000}ms")
    res + res2
  }
}
