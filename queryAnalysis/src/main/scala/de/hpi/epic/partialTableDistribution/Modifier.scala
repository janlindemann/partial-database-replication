package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.utils.{CSVFile, CSVRow}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by Jan on 16.12.2017.
  */
object Modifier {

  def changeWeight(source: CSVFile, seed: Int = 1992): CSVFile = {
    val csvFile = source.copy
    var state = BigDecimal(0)
    var remaining = BigDecimal(1)
    Random.setSeed(seed)
    csvFile.foreach(row => {
      val change = BigDecimal(Random.nextInt(21) - 10) / BigDecimal(100)
      val currentShare = row.getBigDecimal("load")
      if ((state + (currentShare * change)).abs <= remaining * BigDecimal("0.1")) {
        row.setBigDecimal("load", currentShare + (currentShare * change))
        state = state + (currentShare * change)
        remaining = remaining - currentShare
      } else {
        val futurePotential = (remaining - currentShare) * BigDecimal("0.1")
        val overload = (state + (currentShare * change)).abs - futurePotential
        val diff = if ((state + (currentShare * change)).signum == -1) {
          (currentShare * change) + overload
        } else {
          (currentShare * change) - overload
        }
        row.setBigDecimal("load", currentShare + diff)
        state = state + diff
        remaining = remaining - currentShare
      }
    })
    csvFile
  }

  def scale(source: CSVFile, factor: BigDecimal): CSVFile = {
    val csvFile = source.copy
    csvFile.foreach(row => row.setBigDecimal("load", row.getBigDecimal("load") * factor))
    csvFile
  }

  def exchange(source: CSVFile, base: CSVFile, seed: Int = 1992): CSVFile = {
    val result = source.copy

    var previousLoad = BigDecimal(0)
    result.foreach(row => previousLoad += row.getBigDecimal("load"))

    //add rows
    var addedSize = BigDecimal(0)
    result.filter(_.getBigDecimal("load") == BigDecimal(0)).foreach(row => {
      val addSize = base.find(_.getInt("queryID") == row.getInt("queryID")).map(_.getBigDecimal("load")).getOrElse(BigDecimal(0))
      addedSize += addSize
      row.setBigDecimal("load", addSize)
    })

    Random.setSeed(seed)

    //remove rows
    var formerNotZero = result.filter(row => source.find(_.getInt("queryID") == row.getInt("queryID")).exists(_.getBigDecimal("load") != BigDecimal(0)))
    var removedSize = BigDecimal(0)
    while (previousLoad + addedSize - removedSize > BigDecimal(1)) {
      val f = formerNotZero.filter(row => previousLoad + addedSize - removedSize - row.getBigDecimal("load") > BigDecimal(0.95))
      val index = Random.nextInt(f.length)
      val row = formerNotZero.apply(index)
      removedSize += row.getBigDecimal("load")
      row.setBigDecimal("load", BigDecimal(0))
      formerNotZero = formerNotZero.filterNot(_.getInt("queryID") == row.getInt("queryID"))
    }

    result
  }

  def modify(base: CSVFile, source: CSVFile, queryAdd: BigDecimal, queryScale: BigDecimal, maxScale: BigDecimal, seed: Int = 1992): CSVFile = {
    import util.control.Breaks._
    Random.setSeed(seed)
    val result = source.copy
    var load = BigDecimal(0)
    source.foreach(r => load += r.getBigDecimal("load"))
    val newQueries = mutable.ListBuffer.empty[CSVRow]

    if (queryAdd.signum == 1) {
      var added = BigDecimal(0)
      val queriesForSelection =
        base
        .filter(r => {
          source
            .find(_.getInt("queryID") == r.getInt("queryID"))
            .exists(_.getBigDecimal("load") == BigDecimal(0))
        }).to[mutable.ListBuffer]
      breakable {
        while (true) {
          val potential = queriesForSelection.filter(r => added + r.getBigDecimal("load") <= queryAdd)
          if (potential.isEmpty) break
          val random = Random.nextInt(potential.length)
          val selection = potential(random)
          added += selection.getBigDecimal("load")
          newQueries += selection
          queriesForSelection -= selection
        }
      }
    } else if (queryAdd.signum == -1) {
      var removed = BigDecimal(0)
      val queriesForSelection = source.filter(_.getBigDecimal("load") != BigDecimal(0)).to[mutable.ListBuffer]
      breakable {
        while (true) {
          val potential = queriesForSelection.filter(r => removed + r.getBigDecimal("load") <= (queryAdd.abs + BigDecimal("0.025")))
          println(potential.length)
          if (potential.isEmpty) break
          val random = Random.nextInt(potential.length)
          val selection = potential(random)
          removed += selection.getBigDecimal("load")
          result.find(_.getInt("queryID") == selection.getInt("queryID")).foreach(_.setBigDecimal("load", BigDecimal(0)))
          queriesForSelection -= selection
          if (removed > queryAdd.abs) break
        }
      }
    }

    //scaling
    {
      var scaled = BigDecimal(0)
      var added = BigDecimal(0)
      val queries1 = result.filter(_.getBigDecimal("load") != BigDecimal(0)).to[mutable.ListBuffer]
      val queries2 = mutable.ListBuffer.empty[CSVRow]
      while (added < queryScale.abs / maxScale) {
        val next = queries1(Random.nextInt(queries1.length))
        queries1 -= next
        queries2 += next
        added += next.getBigDecimal("load")
      }
      val next = queries1(Random.nextInt(queries1.length))
      queries1 -= next
      queries2 += next
      added += next.getBigDecimal("load")

      var remaining = added
      queries2.foreach(row => {
        val change = (Random.nextDouble * 2 - 1) * maxScale
        val currentShare = row.getBigDecimal("load")
        if (queryScale - scaled <= (remaining * maxScale * queryScale.signum) + (currentShare * change)) {
          row.setBigDecimal("load", currentShare + (currentShare * change))
          scaled = scaled + (currentShare * change)
          remaining = remaining - currentShare
        } else {
          val newScale =  (queryScale - scaled) / remaining
          row.setBigDecimal("load", currentShare + (currentShare * newScale))
          scaled = scaled + (currentShare * newScale)
          remaining = remaining - currentShare
        }
      })


    }

    result.foreach(r => {
      val u = newQueries.find(_.getInt("queryID") == r.getInt("queryID"))
      u.foreach(row => r.setBigDecimal("load", row.getBigDecimal("load")))
    })

    result
  }

  def main(args: Array[String]): Unit = {
    def path(index: Int): String = s"./workloads/tpcds/complete/timings$index.csv"

    val base = CSVFile("./workloads/tpcds/complete/base.csv")
    var f = CSVFile("./workloads/tpcds/complete/timings0.csv")
    val factors = Seq(
      (BigDecimal("0.05"), BigDecimal("0.05"), BigDecimal("0.2")),
      (BigDecimal(0), BigDecimal("-0.05"), BigDecimal("0.2")),
      (BigDecimal("-0.15"), BigDecimal("0"), BigDecimal("0.2")),
      (BigDecimal("-0.05"), BigDecimal("-0.1"), BigDecimal("0.2")),
      (BigDecimal("0.05"), BigDecimal("-0.05"), BigDecimal("0.2")),
      (BigDecimal("0.1"), BigDecimal("0.10"), BigDecimal("0.2")),
      (BigDecimal("-0.05"), BigDecimal("-0.05"), BigDecimal("0.3")),
      (BigDecimal("0.05"), BigDecimal("-0.05"), BigDecimal("0.2")),
      (BigDecimal("0.15"), BigDecimal("0"), BigDecimal("0.2")),
      (BigDecimal("-0.05"), BigDecimal("-0.05"), BigDecimal("0.4"))
    )

    factors.zipWithIndex.foreach(t => {
      val (factor, i) = t
      val res = modify(base, f, factor._1, factor._2, factor._3, 1992 + (i-1) * 3)
      res.save(path(i+1))
      f = res
    })


    /*val base = CSVFile("./workloads/tpch/timings_exchange/timings_base.csv")
    var f = CSVFile("./workloads/tpch/timings_exchange/timings_4.csv")
    for (i <- 5 to 10) {
      val res = exchange(f, base, 1992 + (i-1) * 3)
      res.save(path(i))
      f = res
    }*/

    /*var f = CSVFile("./workloads/tpcds/timings_changing_weight/timings4.csv")
    for (i <- 5 to 10) {
      val res = changeWeight(f, 1992 + (i - 1) * 3)
      res.save(path(i))
      f = res
    }*/
  }
}
