package de.hpi.epic.partialTableDistribution.algorithms

import de.hpi.epic.partialTableDistribution.ElementBasedAnalyzer
import de.hpi.epic.partialTableDistribution.utils.Query

/**
  * Created by Jan on 19.06.2017.
  */
object ParallelBinPacking {
  def main(args: Array[String]): Unit = run

  def comb[T, S, R](f1: T => S, f2: S => R): T => R = (value: T) => f2(f1(value))

  abstract class ParIterator[T] {
    def source = ???
    def map[S](f: T => S): ParIterator[S]
    def fold[B](s: B)(f: (B, T) => B): B
  }

  def run = {
    val t = for {
      i <- 0 until Runtime.getRuntime.availableProcessors()
    } yield new Thread(new Worker(i))

    t.foreach(_.start())
    t.foreach(_.join())
  }

  object Counter {
    var pos = 1
    var a = Array.fill(22)(0)

    def next: Option[Int] = {
      var res = 0
      this.synchronized({
        res = pos
        pos += 1
      })
      if (res <= 22) Some(res) else None
    }

    def n: Seq[Map[Int, Query]] = {

      ???
    }
  }

  class Worker(id: Int) extends Runnable {
    import util.control.Breaks._

    override def run(): Unit = {
      val analyzer = new ElementBasedAnalyzer("./attribute_sizes_new.txt", "./tpc-h_queries_part.txt")
      breakable {
        while (true) {
          val i = Counter.next
          if (i.isEmpty) break;
          println(id, analyzer.evaluate(i.get))
        }
      }
    }
  }
}
