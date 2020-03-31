package com.instagraph.paths.allpairs

import com.instagraph.paths.allpairs.EachSuccessorWeightedInfo.SuccessorsMap
import com.instagraph.paths.ShortestPathsInfo
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

case class EachSuccessorWeighted[V: ClassTag, E: ClassTag](override val graph: Graph[V, E])(implicit numeric: Numeric[E])
  extends AllPairShortestPaths[V, E, EachSuccessorWeightedInfo[E]](reverse = true) {
  type Info = EachSuccessorWeightedInfo[E]
  override type ShortestPathsMap = Map[VertexId, Info]

  override def initialMapping(id: VertexId): ShortestPathsMap =
    Map(id -> EachSuccessorWeightedInfo.toSelf)

  override def messageMap(triplet: EdgeTriplet[ShortestPathsMap, E]): ShortestPathsMap = {
    val edgeCost: E = triplet.attr
    val nextId: VertexId = triplet.srcId
    val nextMap: ShortestPathsMap = triplet.srcAttr
    val originMap: ShortestPathsMap = triplet.dstAttr
    nextMap.map { case(destinationId, nextInfo) =>
      val originValue: Option[Info] = originMap.get(destinationId)
      val nextCost: E = numeric.plus(nextInfo.totalCost, edgeCost)
      val presences: Int = Option(nextInfo.successors.values.sum).filter(p => p > 0).getOrElse(1)
      /*
       * if the origin has not yet a path heading towards that destination, this is obviously the shortest one
       * otherwise we check whether the cost of the current path is better than the current cost from the origin and:
       * - if the cost is higher or the vertex is already contained in the successors list with the same number
       *   of presences, namely there have been found no new shortest paths passing through this one, we reject it
       * - if the cost is lower we create a new path including the next vertex only into the list of successors
       * - if the cost is the same we add the next vertex into the list of successors
       *
       * > to be noted that the number of presences is computed as the sum of the presences of the successors of
       *   the current successor node, and if this node has no successors then the value of is 1
       */
      val updatedInfo: Option[Info] = originValue match {
        case None => Option(EachSuccessorWeightedInfo(nextId, presences, nextCost))
        case Some(originInfo) =>
          val currentPresences = originInfo.successors.getOrElse(nextId, 0)
          if (currentPresences == presences || numeric.gt(nextCost, originInfo.totalCost)) Option.empty
          else if (numeric.lt(nextCost, originInfo.totalCost)) Option(EachSuccessorWeightedInfo(nextId, presences, nextCost))
          else Option(originInfo.addSuccessors((nextId, presences)))
      }
      (destinationId, updatedInfo)
    }.filter { case(_, updatedInfo) => updatedInfo.isDefined }
    .map { case(id, updatedInfo) => (id, updatedInfo.get) }
  }

  override def mergeSameCost(mInfo: Info, nInfo: Info): Info =
    mInfo.addSuccessors(nInfo.successors.toList: _*)
}

object EachSuccessorWeightedInfo {
  type SuccessorsMap = Map[VertexId, Int]

  def apply[C](nextVertex: VertexId, presences: Int, totalCost: C): EachSuccessorWeightedInfo[C] =
    EachSuccessorWeightedInfo(totalCost, Map(nextVertex -> presences))

  def toSelf[C](implicit numeric: Numeric[C]): EachSuccessorWeightedInfo[C] =
    EachSuccessorWeightedInfo(numeric.zero, Map.empty)
}

case class EachSuccessorWeightedInfo[C](override val totalCost: C, successors: SuccessorsMap) extends ShortestPathsInfo[C] {
  def addSuccessors(s: (VertexId, Int)*): EachSuccessorWeightedInfo[C] =
    EachSuccessorWeightedInfo(totalCost, successors ++ s.toMap)
}