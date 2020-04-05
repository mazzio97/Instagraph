package com.instagraph.paths.allpairs.adjacents

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class SinglePath[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val backwardPath: Boolean = false
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, SinglePathInfo[E]] {
  type Info = SinglePathInfo[E]

  override protected def updateInfo(adjacentId: Option[VertexId], cost: E, adjacentInfo: Option[Info]): Info =
    SinglePathInfo(cost, adjacentId)

  // if the adjacentId is the same adjacent vertex stored in the info of the first vertex there is no need to update
  override protected def sendingSameInfo(adjacentId: VertexId, updatedFirstInfo: Info, currentFirstInfo: Info): Boolean =
    currentFirstInfo.adjacentVertex.contains(adjacentId)

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option.empty
}