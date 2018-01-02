package de.hpi.epic.partialTableDistribution.data

/**
  * Created by Jan on 16.12.2017.
  */
case class Column(name: String, length: Int, rows: Int = 0) extends Fragment {
  val size: Long = length * rows
}
