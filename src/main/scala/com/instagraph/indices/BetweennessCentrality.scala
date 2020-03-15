package com.instagraph.indices

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object BetweennessCentrality {

  implicit class BetweennessCentrality[V: ClassTag, E: ClassTag](val graph: Graph[V, E]) {

    def betweennessCentrality: Graph[(V, Double), E] = {
      graph.mapVertices((_, value) => (value, 0.0))
    }
  }
}
