package com.instagraph.paths.allpairs.adjacents

import com.instagraph.paths.allpairs.ShortestPathsInfo
import org.apache.spark.graphx.VertexId

/**
 * Data structure to represent info about the cost and the actual paths of the shortest paths between a pair of vertices
 *
 * @tparam C the type of edge/cost
 */
trait ShortestPathsWithAdjacentVerticesInfo[+C] extends ShortestPathsInfo[C] {
  def adjacentVertices: Set[VertexId]
}

case class EachPathInfo[+C](override val totalCost: C, adjacentSet: Set[VertexId])
  extends ShortestPathsWithAdjacentVerticesInfo[C] {
  def addAdjacentVertices(s: VertexId*): EachPathInfo[C] = EachPathInfo(totalCost, adjacentSet ++ s.toSet)
  override def adjacentVertices: Set[VertexId] = adjacentSet
}

case class EachPathWeightedInfo[+C](override val totalCost: C, adjacentMap: Map[VertexId, Int])
  extends ShortestPathsWithAdjacentVerticesInfo[C] {
  val totalPresences: Int = Option(adjacentMap.values.sum).filter(p => p > 0).getOrElse(1)
  def addAdjacentVertices(s: (VertexId, Int)*): EachPathWeightedInfo[C] = EachPathWeightedInfo(totalCost, adjacentMap ++ s.toMap)
  override def adjacentVertices: Set[VertexId] = adjacentMap.keySet
}

case class SinglePathInfo[+C](override val totalCost: C, adjacentVertex: Option[VertexId])
  extends ShortestPathsWithAdjacentVerticesInfo[C] {
  override def adjacentVertices: Set[VertexId] = adjacentVertex.toSet
}