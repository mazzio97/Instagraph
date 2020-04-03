package com.instagraph.paths.info

/**
 * Data structure to represent info about the cost of the shortest paths between a pair of vertices
 *
 * @tparam C the edge/cost type
 */
trait ShortestPathsInfo[+C] {
  val totalCost: C
}

case class CostOnlyInfo[C](override val totalCost: C) extends ShortestPathsInfo[C]