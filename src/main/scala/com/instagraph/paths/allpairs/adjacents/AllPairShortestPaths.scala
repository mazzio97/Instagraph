package com.instagraph.paths.allpairs.adjacents

import com.instagraph.paths.info.ShortestPathsInfo
import com.instagraph.utils.MapUtils.Manipulations
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Abstract class that computes the shortest paths from each vertex to each other vertex.
 *
 * At the end of the computation, each vertex v will contain in itself a map that associates to each other vertex w the
 * cost of the shortest path (v, w) if at least a path exists, otherwise the vertex w will not be in the map.
 *
 * Additional information can be stored into the map depending on the implementation, such as one or every possible
 * adjacent vertex that will lead to the last vertex w from the first vertex v so that the entire path can be
 * reconstructed by following those adjacent vertices in order.
 *
 * In particular, if the value of the field 'direction' is:
 * - 'FromOrigin' we consider the first vertex (namely the vertices of the output graph) to be the origin of the paths,
 *   the adjacent vertex to be the successor and the last vertex (namely the key of the map) to be the destination
 * - 'FromDestination' we consider the first vertex (namely the vertices of the output graph) to be the destination of
 *   the paths, the adjacent vertex to be the predecessors and the last vertex (namely the key of the map) to be the
 *   origin.
 *
 * @tparam V the vertex type
 * @tparam E the edge type
 * @tparam I the info type
 */
abstract class AllPairShortestPaths[V: ClassTag, E: ClassTag, I <: ShortestPathsInfo[E]: ClassTag]
  (implicit numeric: Numeric[E]) extends Serializable {
  type ShortestPathsMap = Map[VertexId, I]

  protected val graph: Graph[V, E]
  protected val direction: PathsDirection

  /**
   * @param adjacentId the adjacent vertex (of Option.empty if the info is for the first vertex itself)
   * @param cost the cost of the path
   * @param adjacent the info of the adjacent vertex itself
   * @return an info object to be stored as value under the key of the given vertex into another vertex map
   */
  protected def infoAbout(adjacentId: Option[VertexId], cost: E, adjacent: Option[I]): I

  /**
   * @param adjacentId the adjacent vertex
   * @param updatedAdjacentInfo the updated info of the adjacent vertex
   * @param firstInfo the info of the first vertex of the path
   * @return true if the info of the adjacent vertex has not been updated, false otherwise
   */
  protected def sendingSameInfo(adjacentId: VertexId, updatedAdjacentInfo: I, firstInfo: I): Boolean

  /**
   * @param mInfo the info about a certain vertex on a certain shortest path between two vertices
   * @param nInfo the info about a different vertex on another shortest path between the same vertices with the same cost
   * @return the merged info for the shortest paths between those two vertices (Option.empty they can be considered
   *         to be equal and the choice is left to the abstract class itself)
   */
  protected def mergeSameCost(mInfo: I, nInfo: I): Option[I]

  private final def toSelf(vertexId: VertexId): I =
    infoAbout(Option.empty, numeric.zero, Option.empty)

  private final def startPregel(graph: Graph[V, E]): Graph[ShortestPathsMap, E] = {
    val vertexProgram: (VertexId, ShortestPathsMap, ShortestPathsMap) => ShortestPathsMap =
      (_, vertexMap, receivedMap) => vertexMap ++ receivedMap

    val sendMessage: EdgeTriplet[ShortestPathsMap, E] => Iterator[(VertexId, ShortestPathsMap)] =
      triplet => {
        val edgeCost: E = triplet.attr
        val adjacentId: VertexId = triplet.srcId
        val adjacentMap: ShortestPathsMap = triplet.srcAttr
        val firstMap: ShortestPathsMap = triplet.dstAttr
        val messageMap: ShortestPathsMap = adjacentMap.map { case(lastId, adjacentInfo) =>
          /*
           * if the first vertex has no paths heading towards that last one (case None), this is obviously the shortest
           * otherwise we check whether the cost of the current path is better than the current cost from the first and:
           * - if the information turns out not to have been updated we reject the new info
           * - if the cost is higher we reject it as well
           * - if the cost is lower we create a new info object for the path including the adjacent vertex only
           * - if the cost is the same we merge the current info with the updated one
           */
          val firstInfo: Option[I] = firstMap.get(lastId)
          val adjacentCost: E = numeric.plus(adjacentInfo.totalCost, edgeCost)
          val updatedAdjacentInfo: I = infoAbout(Option(adjacentId), adjacentCost, Option(adjacentInfo))
          val updatedLastInfo: Option[I] = firstInfo match {
            case None => Option(updatedAdjacentInfo)
            case Some(originInfo) =>
              if (sendingSameInfo(adjacentId, updatedAdjacentInfo, originInfo)) Option.empty
              else if (numeric.gt(adjacentCost, originInfo.totalCost)) Option.empty
              else if (numeric.lt(adjacentCost, originInfo.totalCost)) Option(updatedAdjacentInfo)
              else mergeSameCost(originInfo, updatedAdjacentInfo)
          }
          (lastId, updatedLastInfo)
        }.filter { case(_, updatedLastInfo) => updatedLastInfo.isDefined }
          .map { case(id, updatedLastInfo) => (id, updatedLastInfo.get) }
        if (messageMap.isEmpty) Iterator.empty else Iterator((triplet.dstId, messageMap))
      }

    /*
     * assuming that both of the two maps are in a shortest paths between the same origin/destination pair, we check
     * whether the cost of the first path(s) is better than the second one and if it is:
     * - higher, we use the other path(s)
     * - lower, we use this one's path(s)
     * - the same, we delegate it to each implementation (if Option.empty is returned we choose the first one)
     */
    val mergeMessages: (ShortestPathsMap, ShortestPathsMap) => ShortestPathsMap =
      (m, n) => m.merge(n, { case (_, nInfo, mInfo) =>
        if (numeric.gt(mInfo.totalCost, nInfo.totalCost)) nInfo
        else if (numeric.lt(mInfo.totalCost, nInfo.totalCost)) mInfo
        else mergeSameCost(mInfo, nInfo).getOrElse(mInfo)
      })

    graph.mapVertices((id, _) => Map(id -> toSelf(id))).pregel[ShortestPathsMap](initialMsg = Map.empty)(
      vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
    )
  }

  final def computeAPSP: Graph[ShortestPathsMap, E] =
    if (direction == FromOrigin) startPregel(graph.reverse).reverse else startPregel(graph)
}

sealed trait PathsDirection
case object FromOrigin extends PathsDirection
case object FromDestination extends PathsDirection