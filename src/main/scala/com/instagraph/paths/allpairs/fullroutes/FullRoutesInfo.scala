package com.instagraph.paths.allpairs.fullroutes

import com.instagraph.paths.allpairs.ShortestPathsInfo
import org.apache.spark.graphx.VertexId

/**
 * Data structure to represent info about the cost and the actual full shortest path(s) between a pair of vertices
 *
 * @tparam C the type of edge/cost
 */
trait FullRoutesInfo[+C] extends ShortestPathsInfo[C] {
  def paths: Set[List[VertexId]]
}

case class SingleFullRouteInfo[+C](override val totalCost: C, path: List[VertexId]) extends FullRoutesInfo[C] {
  override def paths: Set[List[VertexId]] = Set(path)
}

case class EachFullRouteInfo[+C](override val totalCost: C, override val paths: Set[List[VertexId]])
  extends FullRoutesInfo[C]