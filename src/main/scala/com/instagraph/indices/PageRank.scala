package com.instagraph.indices

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object PageRank {
  implicit class PageRank[V: ClassTag, E: ClassTag](val graph: Graph[V, E]) {
    def pageRank(tol: Double): Graph[(V, Double), E] = { Graph(
      graph.ops.pageRank(tol)
        .vertices.join(graph.vertices)
        .mapValues(_.swap),
      graph.edges
    ) }
  }
}