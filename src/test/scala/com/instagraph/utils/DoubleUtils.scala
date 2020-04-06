package com.instagraph.utils

object DoubleUtils {
  implicit class DoubleUtils(value: Double) {
    def similarTo(tolerantDouble: TolerantDouble): Boolean = tolerantDouble == value
  }

  case class TolerantDouble(value: Double, tolerance: Double = 1e-3) {
    def ==(other: Double): Boolean = Math.abs(value - other) <= tolerance
  }
}