package org.urbcomp.startdb.stkq

class ZIndexRange(x: Long, y: Long) {
  val low: Long = x
  val high: Long = y

  def getLow: Long = low
  def getHigh: Long = high
}
