package com.instagraph.paths.info

import org.apache.spark.graphx.VertexId

/**
 * Data structure to represent info about the cost and the actual full shortest path(s) between a pair of vertices
 *
 * @tparam C the type of edge/cost
 */
trait FullPathsInfo[+C] {
  val cost: C
  def paths: Set[List[VertexId]]
}

case class SingleFullPathInfo[+C](override val cost: C, path: List[VertexId]) extends FullPathsInfo[C] {
  override def paths: Set[List[VertexId]] = Set(path)
}

case class EachFullPathInfo[+C](override val cost: C, override val paths: Set[List[VertexId]])
  extends FullPathsInfo[C]