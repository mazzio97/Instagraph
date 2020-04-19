package com.instagraph.paths.allpairs.adjacents

import com.instagraph.paths.allpairs.ShortestPathsInfo
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
 * In particular, if backwardPath is true we consider the first vertex (namely the vertices of the output graph)
 * to be the origin of the paths, the adjacent vertex to be the successor and the last vertex (namely the key of the map)
 * to be the destination. Otherwise, the first vertex (namely the vertices of the output graph) to be the destination of
 * the paths, the adjacent vertex to be the predecessors and the last vertex (namely the key of the map) to be the
 * origin.
 *
 * @tparam V the vertex type
 * @tparam E the edge type
 * @tparam I the info type
 */
abstract class AllPairShortestPaths[V: ClassTag, E: ClassTag, I <: ShortestPathsInfo[E]: ClassTag]
  (implicit numeric: Numeric[E]) extends Serializable {
  type ShortestPathsMap = Map[VertexId, I]

  protected val graph: Graph[V, E]
  protected val backwardPath: Boolean

  /**
   * Update the info of the dst vertex of an edge with respect to the info of the src vertex (called the adjacent).
   *
   * @param adjacentId the id of the adjacent vertex (or Option.empty if the info is for the calling vertex itself)
   * @param cost the cost of the path from a vertex in adjacent map to dst
   * @param adjacentInfo the info about a particular vertex the adjacent currently owns
   * @return an info object to be stored as value under the key of the given vertex into dst vertex map
   */
  protected def updateInfo(adjacentId: Option[VertexId], cost: E, adjacentInfo: Option[I]): I

  /**
   * Check whether the next vertex in the path has already received the info you intend to send.
   *
   * @param adjacentId the adjacent vertex
   * @param updatedFirstInfo the updated info of the adjacent vertex
   * @param currentFirstInfo the info of the first vertex of the path
   * @return true if the info of the adjacent vertex has not been updated, false otherwise
   */
  protected def sendingSameInfo(adjacentId: VertexId, updatedFirstInfo: I, currentFirstInfo: I): Boolean

  /**
   * The logic to adopt when info of two paths of same cost reach a vertex from the same origin.
   *
   * @param mInfo the info about a certain vertex on a certain shortest path between two vertices
   * @param nInfo the info about a different vertex on another shortest path between the same vertices with the same cost
   * @return the merged info for the shortest paths between those two vertices (Option.empty they can be considered
   *         to be equal and the choice is left to the abstract class itself)
   */
  protected def mergeSameCost(mInfo: I, nInfo: I): Option[I]

  private final val toSelf: I = updateInfo(Option.empty, numeric.zero, Option.empty)

  private final def startPregel(graph: Graph[V, E]): Graph[ShortestPathsMap, E] = {
    /*
     * if a message arrives in a vertex it contains meaningful information so the map can be updated
     * without any further checks.
     */
    val vertexProgram: (VertexId, ShortestPathsMap, ShortestPathsMap) => ShortestPathsMap =
      (_, vertexMap, receivedMap) => vertexMap ++ receivedMap

    /*
     * first refers to the dst vertex while adjacent to the src vertex:
     * we must determine if the information contained in an adjacent vertex is meaningful for updating
     * the information of a first vertex and only in this case we must send from adjacent to first
     * an SPMap containing exclusively the paths to update.
     */
    val sendMessage: EdgeTriplet[ShortestPathsMap, E] => Iterator[(VertexId, ShortestPathsMap)] =
      triplet => {
        val edgeCost: E = triplet.attr
        val adjacentId: VertexId = triplet.srcId
        val adjacentMap: ShortestPathsMap = triplet.srcAttr
        val firstMap: ShortestPathsMap = triplet.dstAttr
        val messageMap: ShortestPathsMap = adjacentMap.map { case (lastId, adjacentInfo) =>
          /*
           * if the first vertex has no paths heading towards that last one (case None), this is obviously the shortest
           * otherwise we check whether the cost of the current path is better than the current cost from the first and:
           * - if the information turns out not to have been updated we reject the new info (to avoid infinite loops)
           * - if the cost is higher we reject it as well
           * - if the cost is lower we create a new info object for the path including the adjacent vertex only
           * - if the cost is the same we merge the current info with the updated one
           *
           * notice that, as well as in the rest of the code, with first we intend the vertices of the output graph
           * while with last the vertices that are keys of the shortest paths map, in particular we have that:
           * - if backwardPath is set to true, the so-called first vertex is the destination of the path while the last
           *   vertex is the origin (adjacent vertices are the predecessors of the destination towards the origin)
           * - if backwardPaths is set to false, the so-called first vertex is the origin of the path while the last
           *   vertex is the destination (adjacent vertices are the successors of the origin towards the destination)
           */
          val optionLastInfo: Option[I] = firstMap.get(lastId)
          val adjacentCost: E = numeric.plus(adjacentInfo.totalCost, edgeCost)
          val lastInfoFromAdjacent: I = updateInfo(Option(adjacentId), adjacentCost, Option(adjacentInfo))
          val updatedLastInfo: Option[I] = optionLastInfo match {
            case None => Option(lastInfoFromAdjacent)
            case Some(lastInfo) =>
              if (sendingSameInfo(adjacentId, lastInfoFromAdjacent, lastInfo)) Option.empty
              else if (numeric.gt(adjacentCost, lastInfo.totalCost)) Option.empty
              else if (numeric.lt(adjacentCost, lastInfo.totalCost)) Option(lastInfoFromAdjacent)
              else mergeSameCost(lastInfo, lastInfoFromAdjacent)
          }
          (lastId, updatedLastInfo)
        }.filter { case (_, info) => info.isDefined }
          .mapValues(info => info.get)
          .map(identity)  // https://github.com/scala/bug/issues/7005
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

    graph.mapVertices((id, _) => Map(id -> toSelf)).pregel[ShortestPathsMap](initialMsg = Map.empty)(
      vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
    )
  }

  final def compute: Graph[ShortestPathsMap, E] =
    if (!backwardPath) startPregel(graph.reverse).reverse else startPregel(graph)
}