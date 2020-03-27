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
              .mapValues(nextInfo => ShortestPathInfo(nextId, numeric.plus(nextInfo.totalCost, edgeCost)))
              .filter { case(destinationId, info) =>
                val originValue = originMap.get(destinationId)
                originValue.isEmpty || numeric.lt(info.totalCost, originValue.get.totalCost)
              }
          if (resultMap.isEmpty) Iterator.empty else Iterator((originId, resultMap))
        }
      
      val mergeMessages: (Map[VertexId, ShortestPathInfo[E]], Map[VertexId, ShortestPathInfo[E]]) => Map[VertexId, ShortestPathInfo[E]] =
        (m, n) => (m.keySet ++ n.keySet).map(key => {
          val mValue: Option[ShortestPathInfo[E]] = m.get(key)
          val nValue: Option[ShortestPathInfo[E]] = n.get(key)
          val value = if (mValue.isEmpty) nValue.get else if (nValue.isEmpty) mValue.get else {
            val mInfo = mValue.get
            val nInfo = nValue.get
            if (numeric.lt(mInfo.totalCost, nInfo.totalCost)) mInfo else nInfo
          }
          (key, value)
        }).toMap

      initialGraph.pregel[Map[VertexId, ShortestPathInfo[E]]](initialMsg = Map.empty)(
        vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
      )
    }
  }
}
