package de.janl.simpleBench

import java.sql.DriverManager
import java.sql.Connection

import de.janl.simpleBench.tpch.queries.{Query1, RefreshFunction1, RefreshFunction2}

/**
  * Created by Jan on 01.11.2017.
  */
object Bench {
  def main(args: Array[String]): Unit = {
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost/tpc-h"
    val username = "postgres"
    val password = ""

    var connection: Connection = null
    val queries = Query.load("D:/Downloads/shared_folder/tpc-h/2.17.3/gen_queries_psql") ++ Seq(
      new RefreshFunction1("D:/Downloads/shared_folder/tpc-h/2.17.3/gen_data/orders.tbl.u1", "D:/Downloads/shared_folder/tpc-h/2.17.3/gen_data/lineitem.tbl.u1"),
      new RefreshFunction2("D:/Downloads/shared_folder/tpc-h/2.17.3/gen_data/delete.1")
    )

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val timings = queries.map(_.execute(connection))
      queries.zip(timings).foreach(t => println(s"${t._1.id}: ${t._2}"))
    } finally {
      connection.close()
    }
  }
}
