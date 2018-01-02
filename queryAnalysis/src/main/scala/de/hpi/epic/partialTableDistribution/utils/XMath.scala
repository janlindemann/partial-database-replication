package de.hpi.epic.partialTableDistribution.utils

import scala.annotation.tailrec

/**
  * Created by Jan on 10.07.2017.
  */
object XMath {
  implicit class LongOp(val l: Long) extends AnyVal {
    def ^^(o: Long): Long = pow(l,o)
  }

  @tailrec
  def faculty(l: Long, s : Long = 1l): Long = {
    if (l <= 0) s else faculty(l - 1, l * s)
  }

  final def pow(base:Long, ex:Long):Long = if (ex < 0L) {
    if(base == 0L) sys.error("zero can't be raised to negative power")
    else if (base == 1L) 1L
    else if (base == -1L) if ((ex & 1L) == 0L) -1L else 1L
    else 0L
  } else {
    _pow(1L, base, ex)
  }

  @tailrec
  private final def _pow(t:Long, b:Long, e:Long): Long = {
    if (e == 0L) t
    else if ((e & 1) == 1) _pow(t * b, b * b, e >> 1L)
    else _pow(t, b * b, e >> 1L)
  }

  @tailrec
  def SumRange(from: Long, to: Long, formulae: Long => Long, value: Long = 0l): Long =
    if (from > to) value else SumRange(from + 1, to, formulae, value + formulae(from))
}
