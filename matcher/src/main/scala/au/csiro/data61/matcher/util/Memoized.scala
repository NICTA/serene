package au.csiro.data61.matcher.util

import scala.io._

case class Memoized1[-T,+R](f: T => R) extends (T => R) {
  private[this] val vals = scala.collection.mutable.Map.empty[T,R]
  def apply(x: T): R = {
    vals.getOrElseUpdate(x, f(x))
  }
}

case class Memoized2[-T1,-T2,+R](f: (T1, T2) => R) extends ((T1,T2) => R) {
  private[this] val vals = scala.collection.mutable.Map.empty[(T1,T2),R]
  def apply(x1: T1, x2: T2): R = {
    vals.getOrElseUpdate((x1,x2), f(x1,x2))
  }
}