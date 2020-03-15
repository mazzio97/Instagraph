package com.instagraph.indices

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object PageRank {

  implicit class PageRank[V: ClassTag, E: ClassTag](val graph: Graph[V, E]) {

    def pageRank: Graph[(V, Double), E] = {
      graph.mapVertices((_, value) => (value, 0.0))
    }
  }
}