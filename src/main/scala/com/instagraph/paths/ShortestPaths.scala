package com.instagraph.paths

import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {

  implicit class FewestHops[V: ClassTag, E: ClassTag](val graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = {
      lib.ShortestPaths.run(graph, landmarks)
    }
  }

  implicit class Dijkstra[V: ClassTag](graph: Graph[V, Double]) {
    def dijkstra(origin: VertexId): Graph[(V, Double, List[VertexId]), Double] = {
      var g2 = graph.mapVertices((id, minDist) =>
        (false, if (id == origin) 0 else Double.PositiveInfinity, List[VertexId]())
      )
      for (i <- 1L to graph.vertices.count - 1) {
        val currentVertexId = g2.vertices
          .filter { case (id, (expanded, minDist, visitedNodes)) => !expanded }
          .fold((0L, (false, Double.PositiveInfinity, List[VertexId]())))((a, b) => // Select vertex at min distance
            if (a._2._2 < b._2._2) a else b
          )._1
        val newDistances = g2.aggregateMessages[(Double, List[VertexId])](sendMsg = triplet =>
          if (triplet.srcId == currentVertexId) {
            triplet.sendToDst(
              (triplet.srcAttr._2 + triplet.attr, // Update distances of neighbors of current vertex
                triplet.srcAttr._3 :+ triplet.srcId) // Add current vertex to the path from origin to dst
            )
          },
          mergeMsg = (a, b) => if (a._1 < b._1) a else b // Consider only the message which minimizes the distance
        )
        // Modify the graph with this iteration computed vertices
        g2 = g2.outerJoinVertices(newDistances) { case (id, (expanded, minDist, visitedNodes), vertexUpdate) =>
          val newValue = vertexUpdate.getOrElse((Double.PositiveInfinity, List[VertexId]()))
          (expanded || id == currentVertexId, // Vertex is marked as visited
            math.min(minDist, newValue._1), // The minimum distance is assigned
            if (minDist < newValue._1) visitedNodes else newValue._2) // Take care of the correct path
        }
      }
      graph.outerJoinVertices(g2.vertices) { case (id, original, computed) =>
        val result = computed.getOrElse((false, Double.PositiveInfinity, List[VertexId]()))
        (original, result._2, result._3)
      }
    }
  }
}
