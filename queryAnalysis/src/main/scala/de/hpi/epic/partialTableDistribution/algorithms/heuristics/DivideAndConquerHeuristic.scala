package de.hpi.epic.partialTableDistribution.algorithms.heuristics
import de.hpi.epic.partialTableDistribution.data.{Application, Classification, Fragment, Side}
import de.hpi.epic.partialTableDistribution.utils.toByteConverter

import scala.annotation.tailrec

/**
  * Created by Jan on 27.09.2017.
  */
class DivideAndConquerHeuristic extends Heuristic {
  def recursiveDivision(remainingApplications: Seq[Application], left: XSide, right: XSide, splittedApp: Application): Option[Seq[XSide]] = {
    remainingApplications match {
      case head :: tail =>
        val leftRes = if (left.fits(head)) recursiveDivision(tail, left.append(head), right, splittedApp) else None
        val rightRes = if (right.fits(head)) recursiveDivision(tail, left, right.append(head), splittedApp) else None
        (leftRes, rightRes) match {
          case (Some(l), Some(r)) => if (l.map(_.size).sum < r.map(_.size).sum) Some(l) else Some(r)
          case (Some(l), None) => Some(l)
          case (None, Some(r)) => Some(r)
          case (None, None) => None
        }
      case _ => Some(Seq(left.fill(splittedApp), right.fill(splittedApp)))
    }
  }

  @tailrec
  private def lg(i: Int, r: Int = 0): Int = if (i > 0) lg(i >> 1, r + 1) else r

  private def splitDecision(i: Int): Int = {
    val res = math.pow(2, lg(i) - 1).toInt
    if (res == i) res/2 else res
    //i / 2
  }

  def divide(applications: Seq[Application], sides: Seq[Side]): Map[Side, Seq[Fragment]] = {
    val splitValue = splitDecision(sides.length)
    val res = if (applications.length > 1) {
      applications.flatMap(splittedApp => {
        val remainingApplications = applications.filter(_ != splittedApp)
        val left = new XSide(sides.take(splitValue))
        val right = new XSide(sides.drop(splitValue))
        if (left.fits(remainingApplications.head))
          recursiveDivision(remainingApplications.tail, left.append(remainingApplications.head), right, splittedApp)
        else if (right.fits(remainingApplications.head))
          recursiveDivision(remainingApplications.tail, left, right.append(remainingApplications.head), splittedApp)
        else
          None
      })
    } else {
      val left = new XSide(sides.take(splitValue))
      val right = new XSide(sides.drop(splitValue))
      Seq(Seq(left.fill(applications.head), right.fill(applications.head)))
    }
    val result = res.minBy(_.map(_.size).sum)
    result.par.flatMap(xs => {
      if (xs.bases.length == 1) {
        println(s"${xs.name}: ${xs.size.GB} => ${xs.apps.map(_.name).sortBy(_.toInt).mkString(", ")}")
        Seq((xs.bases.head, xs.apps.flatMap(_.fragments).distinct))
      } else {
        divide(xs.apps, xs.bases).toSeq
      }
    }).seq.toMap
  }

  override def distribute(applications: Seq[Application], sides: Seq[Side]): Map[Side, Seq[Fragment]] = {
    divide(applications, sides)
  }
}

class XApplication(val app: Application, val load: BigDecimal) extends Application { self =>
  def this(app: Application) = this(app, app.load)
  override def classification: Classification = app.classification
  override def fragments: Seq[Fragment] = app.fragments
  override def name: String = app.name

  def partialLoad(l: BigDecimal): XApplication = new XApplication(self.app, if (l <= self.load) l else self.load)
}

class XSide(val bases: Seq[Side], val apps: Seq[Application] = Seq.empty) extends Side {
  self =>

  override val capacity: BigDecimal = bases.map(_.capacity).sum

  override def name: String = bases.map(_.name).mkString("[", ", ", "]")

  val load: BigDecimal = apps.map(_.load).sum
  lazy val size: Long = apps.flatMap(_.fragments).distinct.map(_.size).sum

  def append(a: Application): XSide = new XSide(self.bases, self.apps :+ a)

  def fill(a: Application): XSide =
    if (self.load >= self.capacity) self
    else new XSide(self.bases, self.apps :+ (a match {
      case xa: XApplication => xa.partialLoad(self.capacity - self.load)
      case _ => new XApplication(a).partialLoad(self.capacity - self.load)
    }))

  def fits(a: Application): Boolean = self.load + a.load <= self.capacity
}