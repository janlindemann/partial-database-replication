package de.hpi.epic.partialTableDistribution

import java.io.{BufferedWriter, File, FileWriter}

import de.hpi.epic.partialTableDistribution.algorithms.clustering.{KMedoids, OverlappingQueries}
import de.hpi.epic.partialTableDistribution.data.{Column, Fragment, Query}
import de.hpi.epic.partialTableDistribution.utils._
import smile.math.distance.Metric

/**
  * Created by Jan on 17.07.2017.
  */
class ClusteringAnalyzer(override val tablesFileName: String,
                         override val queriesFileName: String,
                         override val sharesFileName: String) extends Analyzer {
  def included:Unit = {
    val included = OverlappingQueries.included(queries)
    println(s"${included.size} included queries")
    included.foreach(t => println(s"${t._1.name} -> ${t._2.name}"))
  }

  def jaccardDistance[T](s1: Seq[T], s2: Seq[T]): Double = {
    s1.intersect(s2).size / s1.union(s2).distinct.size.toDouble
  }

  def similarityMatrix: Unit = {
    val sorted = queries.sortBy(_.name.toInt)
    val matrix = sorted.map(q1 => sorted.map(q2 => 1 - jaccardDistance(q1.fragments, q2.fragments)))
    val jMatrix = matrix.map(_.toArray).toArray

    val clusters = smile.clustering.specc(jMatrix, 2)
    println(clusters)
    val lbls = clusters.getClusterLabel
    println(lbls.length)

    val res = lbls.zip(sorted).groupBy(_._1).map(_._2.map(_._2).toSeq).toSeq
    val sizes = calculateSizes(res)
    res.foreach(c => println(c.map(_.name).mkString("[", ", ", "]")))
    println(sizes.map(_.GB).mkString("[", ", ", "]"))
    println(sizes.sum.GB)

    /*val file = new File("./matrix.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    val sep = ';'

    //header
    bw.write("id\\id;")
    bw.write(sorted.map(_.id).mkString(sep.toString))
    bw.write(System.lineSeparator())

    //data
    matrix.zip(sorted).foreach { case (row, query) =>
      bw.write(s"${query.id}$sep")
      bw.write(row.map(cell => f"$cell%,.2f").mkString(sep.toString))
      bw.write(System.lineSeparator())
    }

    bw.close()*/
  }

  private def calculateSizes(d: Seq[Seq[Query]]): Seq[Long] = d.map(_.foldLeft(Set.empty[Fragment])(_ ++ _.fragments).foldLeft(0l)(_ + _.size))

  def kMedoids: Unit = {
    def metric(q1: Query, q2: Query): Double =
      (q1.fragments.diff(q2.fragments) ++ q2.fragments.diff(q1.fragments)).foldLeft(0d)(_ + _.size)
    val clustering = new KMedoids[Query](3, metric)
    val medoids = clustering.train(queries)
    val clusters = clustering.cluster(queries, medoids)
    println(medoids.map(_.name).mkString("[", ", ", "]"))
    val sizes = calculateSizes(clusters)
    clusters.foreach(c => println(c.map(_.name).mkString("[", ", ", "]")))
    println(sizes.map(_.GB).mkString("[", ", ", "]"))
    println(sizes.sum.GB)
  }
}

object ClusteringAnalyzer {
  def main(args: Array[String]): Unit = {
    val analyzer = new ClusteringAnalyzer("./attribute_sizes_new.txt", "./tpc-h_queries_part.txt", "../tpc-h/tpc-h timings monet.csv")
    analyzer.similarityMatrix
  }
}
