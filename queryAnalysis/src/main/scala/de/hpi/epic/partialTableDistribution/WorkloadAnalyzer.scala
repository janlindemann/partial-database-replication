package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.data.Fragment
import de.hpi.epic.partialTableDistribution.utils.CSVFile
import de.hpi.epic.partialTableDistribution.utils.toByteConverter

/**
  * Created by Jan on 16.12.2017.
  */
trait WorkloadAnalyzer {
  self : Workload =>

  def columnAccessDistribution: CSVFile = {
    val emptyMap = tables.view.flatMap(_.columns).distinct.map((_, 0)).toMap[Fragment, Int]
    val accesses = queries.foldLeft(emptyMap)((map, query) => {
      query.fragments.distinct.foldLeft(map)((m, fragment) => {
        m.updated(fragment, m.getOrElse(fragment, 0) + 1)
      })
    })
    val distribution = accesses.view.groupBy(_._2).mapValues(_.size)
    val max = distribution.keys.max

    val data = (0 to max).view.map(i => Array(i.toString, distribution.getOrElse(i, 0).toString)).toArray
    new CSVFile(data, Array("Accesses", "Columns"))
  }

  def memUsage: CSVFile = {
    val totalSize = tables.foldLeft(0l)(_ + _.columns.distinct.foldLeft(0l)(_ + _.size)).toDouble

    val header = Array("query_id", "table_count", "column_count", "mem_usage_tables", "relative_mem_usage_tables",
      "mem_usage_columns", "relative_mem_usage_columns",	"possible_savings")

    val data = queries.view.sortBy(_.name.toInt).map(q => {
      val usedTables = q.fragments.distinct.groupBy(f => tables.find(t => t.columns.contains(f)).get)
      val memTables = usedTables.view.map(t => t._1.columns.foldLeft(0l)(_ + _.size)).sum
      val relativeMemTables = memTables / totalSize
      val memColumns = usedTables.view.map(t => t._2.foldLeft(0l)(_ + _.size)).sum
      val relativeMemColumns = memColumns / totalSize

      Array[String](q.name, usedTables.size.toString, q.fragments.size.toString, memTables.toString,
        relativeMemTables.toString, memColumns.toString, relativeMemColumns.toString,
        (memColumns / memTables.toDouble).toString)
    }).toArray
    new CSVFile(data, header)
  }

  def tableInfo: CSVFile = {
    val totalSize = tables.foldLeft(0l)(_ + _.columns.distinct.foldLeft(0l)(_ + _.size)).toDouble

    val usageCount = queries.flatMap(q => {
      q.fragments.map(f => tables.find(t => t.columns.contains(f)).get).distinct
    }).groupBy(t => t).mapValues(_.length)

    val sizeShare = tables.map(t => (t,(t.columns.distinct.map(_.size).sum / totalSize) * 100)).sortBy(-1 * _._2)

    val data = sizeShare
      .view
      .map(t => Array[String](t._1.name, t._2.toString, usageCount.getOrElse(t._1, 0).toString))
      .sortBy(_.head)
      .toArray
    new CSVFile(data, Array("table_name", "mem_share", "reference_count"))
  }

  def workloadStatistics: Unit = {
    println(s"${queries.length} queries")
    println(s"${tables.length} tables")
    println(s"${tables.map(_.columns.length).sum} columns")
    val total_size = tables.map(_.columns.map(_.size).sum).sum
    println(s"total size: ${total_size.GB}\n")

    val usedColumns = queries.flatMap(_.fragments).distinct
    println(s"${usedColumns.length} used columns")
    val used_size = usedColumns.aggregate(0l)(_ + _.size, _ + _)
    println(s"unused size: ${(total_size - used_size).GB} ")
  }
}
