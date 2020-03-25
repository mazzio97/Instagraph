package com.instagraph.paths

import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {

  implicit class Hops[V: ClassTag, E: ClassTag](val graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = {
      lib.ShortestPaths.run(graph, landmarks)
    }
  }

  implicit class Distances[V: ClassTag](graph: Graph[V, Double]) {
    def dijkstra(origin: VertexId): Graph[(V, (Double, List[VertexId])), Double] = {
      val initialGraph = graph.mapVertices((id, _) =>
        (if (id == origin) 0 else Double.PositiveInfinity, List[VertexId]())
      )
      val shortestPaths = initialGraph.pregel(initialMsg = (Double.PositiveInfinity, List[VertexId]()))(
        vprog = (id, currDist, newDist) => if (currDist._1 < newDist._1) currDist else newDist,
        sendMsg = triplet => {
          if (triplet.srcAttr._1 + triplet.attr < triplet.dstAttr._1) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.srcId)))
          } else {
            Iterator.empty
          }
        },
        mergeMsg = (a, b) => if (a._1 < b._1) a else b
      )
      Graph(shortestPaths.vertices.join(graph.vertices).mapValues(_.swap), graph.edges)
    }
  }
}
