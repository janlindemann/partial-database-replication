package de.hpi.epic.partialTableDistribution

import java.io.{BufferedWriter, File, FileWriter}

import de.hpi.epic.partialTableDistribution.algorithms.{ShareBasedBinPacking, calculatePossibilitiesPerf}
import de.hpi.epic.partialTableDistribution.algorithms.ShareBasedBinPacking.RangeGenerator
import de.hpi.epic.partialTableDistribution.algorithms.heuristics._
import de.hpi.epic.partialTableDistribution.cluster.stream.StreamCluster
import de.hpi.epic.partialTableDistribution.data._
import de.hpi.epic.partialTableDistribution.utils.{time, timeAnd, toByteConverter}

import scala.collection.mutable


/**
  * Created by Jan on 09.07.2017.
  */
class TPCHWorkload(override val tablesFileName: String,
                   override val queriesFileName: String,
                   override val sharesFileName: String ) extends Analyzer with WorkloadAnalyzer with HeuristicEvaluation {

  private def calculateSize(d: Map[Int, Seq[Query]]): Long = d.map(_._2.foldLeft(Set.empty[Fragment])(_ ++ _.fragments).foldLeft(0l)(_ + _.size)).sum
  private def calculateSizes(d:Seq[Seq[Query]]): Seq[Long] = d.map(_.foldLeft(Set.empty[Fragment])(_ ++ _.fragments).foldLeft(0l)(_ + _.size))

  def jaccardDistance[T](s1: Set[T], s2: Set[T]): Double = {
    s1.intersect(s2).size / s1.union(s2).size.toDouble
  }

  def weightedJaccardDistance(s1: Set[Column], s2: Set[Column]): Double = {
    val intersection = s1.intersect(s2).foldLeft(0l)(_ + _.size)
    val union = s1.union(s2).foldLeft(0l)(_ + _.size)
    return intersection / union.toDouble
  }

  def queryCentricPartitioning(backendCount: Int): Unit = {
    val backends = 1 to backendCount map (idx => new Side {
      override def capacity: BigDecimal = BigDecimal(1 / backendCount.toDouble)
      override def name: String = s"B$idx"
    })
    val heuristic = new QueryCentricPartitioningHeuristic()
    val result = heuristic.distribute(queries, backends)
    val sizes = result.values.map(_.distinct.map(_.size).sum)
    val loadMatrix = heuristic.loadMatrix
    val rows = loadMatrix.foldLeft(Map.empty[Side, Seq[String]])((map, entry) => if (entry._2 != 0) map.updated(entry._1._1, map.getOrElse(entry._1._1, Seq.empty[String]) :+ entry._1._2.name) else map)

    println(s"$backendCount replicas\nquery centric partitioning heuristic")
    println(s"${sizes.sum.GB} : ${rows.toSeq.sortBy(_._1.name).map(_._2.mkString("[", ",", "]")).mkString("[", ", ", "]")} ${sizes.map(_.GB).mkString("[", ", ", "]")}")
    println()
  }

  def improvedQCP(backendCount: Int): Unit = {
    /*case class BackendImpl(name: String, capacity: BigDecimal) extends Side
    case class GroupedApplication(queries: Seq[Query], shares: Map[Query, Double]) extends Application {
      override def classification: Classification = if (queries.exists(_.classification == ReadQuery)) ReadQuery else UpdateQuery
      override def fragments: Seq[Fragment] = queries.flatMap(_.fragments).distinct
      override def load: BigDecimal = queries.map(a => shares.getOrElse(a, 0d)).map(a => BigDecimal(a)).sum
      override def name: String = queries.map(_.name).mkString(",")
    }
    class QCPHeuristic(val input: Seq[Query], override val backends: Seq[Side], shares: Map[Query, Double]) extends QueryCentricPartitioningHeuristic[Side, Application, Fragment] {
      override val classifications: Seq[Application] = groupApplications

      override protected def classify(c: Application): ClassificationType = ReadQuery
      override protected def fragments(c: Application): Seq[Fragment] = c.fragments
      override protected def capacity(b: Side): BigDecimal = b.capacity
      override protected def size(f: Fragment): BigDecimal = f.size
      override protected def weigth(c: Application): BigDecimal = c.load

      override protected def backendName(b: Side): String = b.name
      override protected def classificationName(c: Application): String = c.name

      private def singleUsedFragments: Set[Column] = {
        input.foldLeft(Map.empty[Column, Int])((map, query) => {
          query.columns.foldLeft(map)((map, column) => {
            map.updated(column, map.getOrElse(column, 0) + 1)
          })
        }).collect { case (col, count) if count == 1 => col}.toSet
      }

      private def similarityMatrix: Seq[Seq[Double]] = {
        val exclude = singleUsedFragments
        input.map(q1 => input.map(q2 => weightedJaccardDistance(q1.columns.diff(exclude), q2.columns.diff(exclude))))
      }

      private def cluster: Seq[GroupedApplication] = {
        val matrix = similarityMatrix
        val tuples = for (
          i <- similarityMatrix.indices;
          j <- i+1 until similarityMatrix.length;
          if matrix(i)(j) > 0.7d
        ) yield (i,j)
        println(tuples)
        Seq.empty
      }

      private def subquery(q1: Query, q2: Query): Boolean = {
        if (q1.columns == q2.columns) {
          if (q1.id < q2.id) true else false
        } else {
          q1.columns.intersect(q2.columns) == q1.columns
        }
      }

      private def findIncluded: Seq[GroupedApplication] = {
         val relations = input.foldLeft(Map.empty[Query, Seq[Query]])((map, superQuery) => {
           val subQueries = input.filter(subquery(_, superQuery))
           if (subQueries.isEmpty) map
           else map.updated(superQuery, subQueries)
         })
        cluster
         relations
           .filterKeys(key => !relations.exists(_._2.contains(key)))
           .toSeq
           .map(t => GroupedApplication(t._2 :+ t._1, shares))
      }

      private def groupApplications: Seq[GroupedApplication] = {
        val included = findIncluded
        val res = input.collect {
            case q if !included.exists(_.queries.contains(q)) => GroupedApplication(Seq(q), shares)
        } ++ included
        println(res.map(_.name).mkString("[", "| |", "]"))
        res
      }
    }

    val backends = 1 to backendCount map (idx => BackendImpl(s"B$idx", 1 / backendCount.toDouble))
    val heuristic = new QCPHeuristic(queries, backends, shares)
    val result = heuristic.distribute
    val sizes = result.map(t => t._2.distinct.map(_.size).sum)
    println(s"$backendCount server: ${sizes.sum.GB}\nimproved query centric partitioning heuristic")*/
  }
}

object TPCHWorkload {

  def main(args: Array[String]): Unit = {
    val analyzer = new TPCHWorkload( "./workloads/tpch/attribute_sizes_new.txt",
      "./workloads/tpch/tpc-h_queries_part.txt", "./workloads/tpch/tpc-h timings monet.csv" )

    println(analyzer.tables.map(_.columns.length).sum)

    //analyzer.tableUsage
    //analyzer.columnAccessDistribution
    //analyzer.tableInfo

    def weightedJaccardDistance(s1: Seq[Application], s2: Seq[Application]): Double = {
      val fragments1 = s1.flatMap(_.fragments).distinct
      val fragments2 = s2.flatMap(_.fragments).distinct

      val intersection = fragments1.intersect(fragments2).foldLeft(0l)(_ + _.size)
      val union = fragments1.union(fragments2).foldLeft(0l)(_ + _.size)
      intersection / union.toDouble
    }

    def memSize(s1: Seq[Application], s2: Seq[Application]): Double = {
      val fragments1 = s1.flatMap(_.fragments).distinct
      val fragments2 = s2.flatMap(_.fragments).distinct

      val x = fragments1.diff(fragments2).foldLeft(0l)(_ + _.size)
      val y = fragments2.diff(fragments1).foldLeft(0l)(_ + _.size)
      x + y
    }

    analyzer.analyzes(
      Seq(new MaxWeightHeuristic(memSize)),
      "D:/Dokumente/Masterarbeit/thesis/data/tpch", 2, 8)

    //analyzer.improvedQCP(11)
    //analyzer.mkJson()
    //println()
    //1 to 11 map(analyzer.queryCentricPartitioning(_))
  }
}