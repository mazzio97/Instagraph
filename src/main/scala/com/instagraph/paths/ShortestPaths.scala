package com.instagraph.paths

import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {
  implicit class Hops[V: ClassTag, E: ClassTag](val graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = {
      lib.ShortestPaths.run(graph, landmarks)
    }
  }

  implicit class Distances[V: ClassTag](graph: Graph[V, Double]) {
    def allPairsShortestPath(): Graph[Map[VertexId, ShortestPathInfo], Double] = {
      val defaultMap: Map[VertexId, ShortestPathInfo] =
        graph.vertices.mapValues(_ => ShortestPathInfo.noPath).collectAsMap().toMap

      val initialGraph: Graph[Map[VertexId, ShortestPathInfo], Double] =
        graph.reverse.mapVertices((id, _) => defaultMap + (id -> ShortestPathInfo.toSelf))

      val vertexProgram: (VertexId, Map[VertexId, ShortestPathInfo], Map[VertexId, ShortestPathInfo]) => Map[VertexId, ShortestPathInfo] =
        (id, vertexMap, receivedMap) => vertexMap ++ receivedMap

      val sendMessage: EdgeTriplet[Map[VertexId, ShortestPathInfo], Double] => Iterator[(VertexId, Map[VertexId, ShortestPathInfo])] =
        triplet => {
          def edgeCost: Double = triplet.attr
          def nextId: VertexId = triplet.srcId
          def originId: VertexId = triplet.dstId
          def nextMap: Map[VertexId, ShortestPathInfo] = triplet.srcAttr
          def originMap: Map[VertexId, ShortestPathInfo] = triplet.dstAttr
          val resultMap: Map[VertexId, ShortestPathInfo] = nextMap
              .mapValues(nextInfo => ShortestPathInfo.through(nextId, nextInfo.totalCost + edgeCost))
              .filter { case(destinationId, info) => info.totalCost + edgeCost < originMap(destinationId).totalCost }
          if (resultMap.isEmpty) Iterator.empty else Iterator((originId, resultMap))
        }
      
      val mergeMessages: (Map[VertexId, ShortestPathInfo], Map[VertexId, ShortestPathInfo]) => Map[VertexId, ShortestPathInfo] =
        (m, n) => (m.keySet ++ n.keySet).map(key => {
          val mInfo: ShortestPathInfo = m.getOrElse(key, ShortestPathInfo.noPath)
          val nInfo: ShortestPathInfo = n.getOrElse(key, ShortestPathInfo.noPath)
          if (mInfo.totalCost < nInfo.totalCost) (key, mInfo) else (key, nInfo)
        }).toMap

      initialGraph.pregel[Map[VertexId, ShortestPathInfo]](initialMsg = Map.empty[VertexId, ShortestPathInfo])(
        vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
      )
    }
  }
}
