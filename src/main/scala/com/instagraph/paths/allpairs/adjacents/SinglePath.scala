package com.instagraph.paths.allpairs.adjacents

import com.instagraph.paths.info.SinglePathInfo
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class SinglePath[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val direction: PathsDirection
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, SinglePathInfo[E]] {
  type Info = SinglePathInfo[E]

  override protected def infoAbout(adjacentId: Option[VertexId], cost: E, adjacent: Option[Info]): Info =
    SinglePathInfo(cost, adjacentId)

  // if the adjacentId is the same adjacent vertex stored in the info of the first vertex there is not need to update
  override protected def sendingSameInfo(adjacentId: VertexId, updatedAdjacentInfo: Info, firstInfo: Info): Boolean =
    firstInfo.adjacentVertex.contains(adjacentId)

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option.empty
}