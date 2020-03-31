package com.instagraph.paths.allpairs

import com.instagraph.paths.ShortestPathsInfo
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

case class SingleSuccessor[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val direction: PathsDirection
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, SingleSuccessorInfo[E]] {
  type Info = SingleSuccessorInfo[E]

  override protected def infoAbout(adjacentId: Option[VertexId], cost: E, adjacent: Option[Info]): Info =
    SingleSuccessorInfo(cost, adjacentId)

  override protected def dontUpdateInfo(adjacentId: VertexId, updatedAdjacentInfo: Info, firstInfo: Info): Boolean =
    true

  // when considering a single path, if two have the same cost it is enough to follow the first one
  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Info =
    mInfo
}

case class SingleSuccessorInfo[C](override val totalCost: C, adjacentVertex: Option[VertexId]) extends ShortestPathsInfo[C]