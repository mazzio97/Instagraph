package com.instagraph.paths

import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {
  /**
   * Implicit class to compute fewest hops on a graph
   *
   * @param graph the input graph
   * @tparam V the vertex type
   * @tparam E the edge type
   */
  implicit class Hops[V: ClassTag, E: ClassTag](val graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = {
      lib.ShortestPaths.run(graph, landmarks)
    }
  }

  /**
   * Implicit class to compute all pairs shortest path on a graph
   *
   * @param graph the input graph
   * @tparam V the vertex type
   * @tparam E the edge type
   */
  implicit class Distances[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def allPairsShortestPath(implicit numeric: Numeric[E]): Graph[Map[VertexId, ShortestPathInfo[E]], E] = {
      val initialGraph: Graph[Map[VertexId, ShortestPathInfo[E]], E] =
        graph.reverse.mapVertices((id, _) => Map(id -> ShortestPathInfo.toSelf))

      val vertexProgram: (VertexId, Map[VertexId, ShortestPathInfo[E]], Map[VertexId, ShortestPathInfo[E]]) => Map[VertexId, ShortestPathInfo[E]] =
        (_, vertexMap, receivedMap) => vertexMap ++ receivedMap

      val sendMessage: EdgeTriplet[Map[VertexId, ShortestPathInfo[E]], E] => Iterator[(VertexId, Map[VertexId, ShortestPathInfo[E]])] =
        triplet => {
          def edgeCost: E = triplet.attr
          def nextId: VertexId = triplet.srcId
          def originId: VertexId = triplet.dstId
          def nextMap: Map[VertexId, ShortestPathInfo[E]] = triplet.srcAttr
          def originMap: Map[VertexId, ShortestPathInfo[E]] = triplet.dstAttr
          val resultMap: Map[VertexId, ShortestPathInfo[E]] = nextMap
              .map { case(destinationId, nextInfo) =>
                val originValue = originMap.get(destinationId)
                val nextCost = numeric.plus(nextInfo.totalCost, edgeCost)
                /*
                 * if the origin has not yet a path heading towards that destination, this is obviously the shortest one
                 * otherwise we check whether the cost of the current path is better than the current cost from the origin and:
                 * - if the cost is higher (or the vertex is already contained in the successors list) we reject this path
                 * - if the cost is lower we create a new path including the next vertex only into the list of successors
                 * - if the cost is the same we add the next vertex into the list of successors
                 */
                val updatedInfo = if (originValue.isEmpty) {
                  ShortestPathInfo(nextId, nextCost)
                } else {
                  val originInfo = originValue.get
                  if (originInfo.successors.contains(nextId) || numeric.gt(nextCost, originInfo.totalCost)) ShortestPathInfo.toSelf
                  else if (numeric.lt(nextCost, originInfo.totalCost)) ShortestPathInfo(nextId, nextCost)
                  else originInfo.addSuccessors(nextId)
                }
                (destinationId, updatedInfo)
              }.filter { case(_, updatedInfo) => updatedInfo.successors.nonEmpty }
          if (resultMap.isEmpty) Iterator.empty else Iterator((originId, resultMap))
        }
      
      val mergeMessages: (Map[VertexId, ShortestPathInfo[E]], Map[VertexId, ShortestPathInfo[E]]) => Map[VertexId, ShortestPathInfo[E]] =
        (m, n) => (m.keySet ++ n.keySet).map(destinationId => {
          val mValue: Option[ShortestPathInfo[E]] = m.get(destinationId)
          val nValue: Option[ShortestPathInfo[E]] = n.get(destinationId)
          /*
           * if one of the two maps does not have a path for this destination, we return the other path
           * otherwise we check whether the cost of one of the two paths path is better than the other one and:
           * - if the cost is higher we use the other path(s)
           * - if the cost is lower we use this one's path(s)
           * - if the cost is the same we add the successor(s) of the other path(s) into this one's successor list
           */
          val updatedInfo = if (mValue.isEmpty) nValue.get else if (nValue.isEmpty) mValue.get else {
            val mInfo = mValue.get
            val nInfo = nValue.get
            if (numeric.gt(mInfo.totalCost, nInfo.totalCost)) nInfo
            else if (numeric.lt(mInfo.totalCost, nInfo.totalCost)) mInfo
            else mInfo.addSuccessors(nInfo.successors.toList: _*)
          }
          (destinationId, updatedInfo)
        }).toMap

      initialGraph.pregel[Map[VertexId, ShortestPathInfo[E]]](initialMsg = Map.empty)(
        vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
      )
    }
  }
}
