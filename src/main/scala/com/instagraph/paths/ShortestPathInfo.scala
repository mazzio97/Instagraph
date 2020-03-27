package com.instagraph.paths

import org.apache.spark.graphx.VertexId

case class ShortestPathInfo private(totalCost: Double, nextVertex: Option[VertexId])

object ShortestPathInfo {
  val noPath = ShortestPathInfo(Double.PositiveInfinity, Option.empty)

  val toSelf = ShortestPathInfo(0, Option.empty)

  def through(next: VertexId, totalCost: Double) = ShortestPathInfo(totalCost, Option(next))
}