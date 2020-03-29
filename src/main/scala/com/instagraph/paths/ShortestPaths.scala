package com.instagraph.paths

import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {
  type ShortestPathsMap[E] = Map[VertexId, ShortestPathInfo[E]]
  
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
    def allPairsShortestPath(implicit numeric: Numeric[E]): Graph[ShortestPathsMap[E], E] = {
      /*
       * the initial graph has reversed edges so that each node can receive messages from nodes that,
       * in the original graph, were after itself (and not before)
       */
      val initialGraph: Graph[ShortestPathsMap[E], E] =
        graph.reverse.mapVertices((id, _) => Map(id -> ShortestPathInfo.toSelf))

      val vertexProgram: (VertexId, ShortestPathsMap[E], ShortestPathsMap[E]) => ShortestPathsMap[E] =
        (_, vertexMap, receivedMap) => vertexMap ++ receivedMap

      val sendMessage: EdgeTriplet[ShortestPathsMap[E], E] => Iterator[(VertexId, ShortestPathsMap[E])] =
        triplet => {
          def edgeCost: E = triplet.attr
          def nextId: VertexId = triplet.srcId
          def originId: VertexId = triplet.dstId
          def nextMap: ShortestPathsMap[E] = triplet.srcAttr
          def originMap: ShortestPathsMap[E] = triplet.dstAttr
          val resultMap: ShortestPathsMap[E] = nextMap.map { case(destinationId, nextInfo) =>
            val originValue: Option[ShortestPathInfo[E]] = originMap.get(destinationId)
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
            val updatedInfo: Option[ShortestPathInfo[E]] = originValue match {
              case None => Option(ShortestPathInfo(nextId, presences, nextCost))
              case Some(originInfo) =>
                val currentPresences = originInfo.successors.getOrElse(nextId, 0)
                if (currentPresences == presences || numeric.gt(nextCost, originInfo.totalCost)) Option.empty
                else if (numeric.lt(nextCost, originInfo.totalCost)) Option(ShortestPathInfo(nextId, presences, nextCost))
                else Option(originInfo.addSuccessors((nextId, presences)))
            }
            (destinationId, updatedInfo)
          }.filter { case(_, updatedInfo) => updatedInfo.isDefined }
            .map { case(id, updatedInfo) => (id, updatedInfo.get) }
          if (resultMap.isEmpty) Iterator.empty else Iterator((originId, resultMap))
        }
      
      val mergeMessages: (ShortestPathsMap[E], ShortestPathsMap[E]) => ShortestPathsMap[E] =
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

      initialGraph.pregel[ShortestPathsMap[E]](initialMsg = Map.empty)(
        vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
      )
    }
  }
}
