package com.instagraph.paths.allpairs.adjacents

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class EachPathWeighted[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val backwardPath: Boolean = false
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, EachPathWeightedInfo[E]] {
  type Info = EachPathWeightedInfo[E]

  // the number of presences is computed as the sum of the presences of the successors of the current successor node,
  // and if this node has no successors then the value is 1
  override protected def updateInfo(adjacentId: Option[VertexId], cost: E, adjacentInfo: Option[Info]): Info =
    EachPathWeightedInfo(
      cost,
      adjacentId.map(id => Map(id -> adjacentInfo.map(_.totalPresences).getOrElse(1))).getOrElse(Map.empty)
    )

  // if the adjacentId is in the list of adjacent vertices of the first vertex and has the same presences there is no
  // need to update the info (note that if the vertex is not in the list of adjacent vertices the returned value is
  // zero, which will always be different from the number of total presences being them at least 1)
  override protected def sendingSameInfo(adjacentId: VertexId, updatedFirstInfo: Info, currentFirstInfo: Info): Boolean =
    updatedFirstInfo.totalPresences == currentFirstInfo.adjacentMap.getOrElse(adjacentId, 0)

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option(mInfo.addAdjacentVertices(nInfo.adjacentMap.toList: _*))
}
