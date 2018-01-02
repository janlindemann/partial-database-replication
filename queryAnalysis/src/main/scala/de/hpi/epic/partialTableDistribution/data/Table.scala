package de.hpi.epic.partialTableDistribution.data

/**
  * Created by Jan on 16.12.2017.
  */
case class Table(name: String, columns: Seq[Column]) extends Fragment {
  val size : Long = columns.foldLeft(0l)(_ + _.size)
}
