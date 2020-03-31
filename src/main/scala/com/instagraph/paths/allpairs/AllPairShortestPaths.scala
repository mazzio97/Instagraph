package com.instagraph.paths.allpairs

import com.instagraph.paths.ShortestPathsInfo
import com.instagraph.utils.MapUtils.Manipulations
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

abstract class AllPairShortestPaths[V: ClassTag, E: ClassTag, I <: ShortestPathsInfo[E]: ClassTag](protected val reverse: Boolean)
                                                                                                  (implicit numeric: Numeric[E]) extends Serializable {
  type ShortestPathsMap = Map[VertexId, I]

  val graph: Graph[V, E]

  protected def initialMapping(id: VertexId): ShortestPathsMap
  protected def messageMap(triplet: EdgeTriplet[ShortestPathsMap, E]): ShortestPathsMap
  protected def mergeSameCost(mInfo: I, nInfo: I): I

  private final def compute(graph: Graph[V, E]): Graph[ShortestPathsMap, E] = {
    val vertexProgram: (VertexId, ShortestPathsMap, ShortestPathsMap) => ShortestPathsMap =
      (_, vertexMap, receivedMap) => vertexMap ++ receivedMap

    val sendMessage: EdgeTriplet[ShortestPathsMap, E] => Iterator[(VertexId, ShortestPathsMap)] =
      triplet => {
        val map: ShortestPathsMap = messageMap(triplet)
        if (map.isEmpty) Iterator.empty
        else Iterator((triplet.dstId, map))
      }

    /*
     * assuming that both of the two maps have a path for this destination, we check whether the cost of the first
     * path(s) is better than the second one and if it is:
     * - higher, we use the other path(s)
     * - lower, we use this one's path(s)
     * - the same, we delegate it to each implementation
     */
    val mergeMessages: (ShortestPathsMap, ShortestPathsMap) => ShortestPathsMap =
      (m, n) => m.merge(n, { case (_, nInfo, mInfo) =>
        if (numeric.gt(mInfo.totalCost, nInfo.totalCost)) nInfo
        else if (numeric.lt(mInfo.totalCost, nInfo.totalCost)) mInfo
        else mergeSameCost(mInfo, nInfo)
      })

    graph.mapVertices((id, _) => initialMapping(id)).pregel[ShortestPathsMap](initialMsg = Map.empty)(
      vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
    )
  }

  final def allPairsShortestPath: Graph[ShortestPathsMap, E] =
    if (reverse) compute(graph.reverse).reverse else compute(graph)
}