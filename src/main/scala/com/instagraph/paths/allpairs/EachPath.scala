package com.instagraph.paths.allpairs

import com.instagraph.paths.ShortestPathsInfo
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

case class EachPath[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val direction: PathsDirection
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, EachPathInfo[E]] {
  type Info = EachPathInfo[E]

  override protected def infoAbout(adjacentId: Option[VertexId], cost: E, adjacent: Option[Info]): Info =
    EachPathInfo(cost, adjacentId.map(id => Set(id)).getOrElse(Set.empty))

  // if the adjacentId is in the list of adjacent vertices of the first vertex there is not need to update the info
  override protected def sendingSameInfo(adjacentId: VertexId, updatedAdjacentInfo: Info, firstInfo: Info): Boolean =
    firstInfo.adjacentVertices.contains(adjacentId)

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option(mInfo.addAdjacentVertices(nInfo.adjacentVertices.toList: _*))
}

case class EachPathInfo[C](override val totalCost: C, adjacentVertices: Set[VertexId]) extends ShortestPathsInfo[C] {
  def addAdjacentVertices(s: VertexId*): EachPathInfo[C] = EachPathInfo(totalCost, adjacentVertices ++ s.toSet)
}