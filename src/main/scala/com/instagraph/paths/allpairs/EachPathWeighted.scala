package com.instagraph.paths.allpairs

import com.instagraph.paths.{ShortestPathsInfo, ShortestPathsWithAdjacentVerticesInfo}
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

case class EachPathWeighted[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val direction: PathsDirection
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, EachPathWeightedInfo[E]] {
  type Info = EachPathWeightedInfo[E]

  // the number of presences is computed as the sum of the presences of the successors of the current successor node,
  // and if this node has no successors then the value of is 1
  override protected def infoAbout(adjacentId: Option[VertexId], cost: E, adjacent: Option[Info]): Info =
    EachPathWeightedInfo(
      cost,
      adjacentId.map(id => Map(id -> adjacent.map(_.totalPresences).getOrElse(1))).getOrElse(Map.empty)
    )

  // if the adjacentId is in the list of adjacent vertices of the first vertex and has the same presences there is no
  // need to update the info (note that if the vertex is not in the list of adjacent vertices the returned value is
  // zero, which will always be different from the number of total presences being them at least 1)
  override protected def sendingSameInfo(adjacentId: VertexId, updatedAdjacentInfo: Info, firstInfo: Info): Boolean =
    updatedAdjacentInfo.totalPresences == firstInfo.adjacentMap.getOrElse(adjacentId, 0)

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option(mInfo.addAdjacentVertices(nInfo.adjacentMap.toList: _*))
}

case class EachPathWeightedInfo[+C](override val totalCost: C, adjacentMap: Map[VertexId, Int])
  extends ShortestPathsWithAdjacentVerticesInfo[C] {
  val totalPresences: Int = Option(adjacentMap.values.sum).filter(p => p > 0).getOrElse(1)
  def addAdjacentVertices(s: (VertexId, Int)*): EachPathWeightedInfo[C] = EachPathWeightedInfo(totalCost, adjacentMap ++ s.toMap)
  override def adjacentVertices: Set[VertexId] = adjacentMap.keySet
}

