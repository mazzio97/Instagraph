package com.instagraph.paths.allpairs.adjacents

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class EachPath[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val backwardPath: Boolean = false
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, EachPathInfo[E]] {
  type Info = EachPathInfo[E]

  override protected def updateInfo(adjacentId: Option[VertexId], cost: E, adjacentInfo: Option[Info]): Info =
    EachPathInfo(cost, adjacentId.map(id => Set(id)).getOrElse(Set.empty))

  // if the adjacentId is in the list of adjacent vertices of the first vertex there is no need to update the info
  override protected def sendingSameInfo(adjacentId: VertexId, updatedFirstInfo: Info, currentFirstInfo: Info): Boolean =
    currentFirstInfo.adjacentSet.contains(adjacentId)

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option(mInfo.addAdjacentVertices(nInfo.adjacentSet.toList: _*))
}