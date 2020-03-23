package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object BetweennessCentrality extends CentralityIndex[Double] {
  // TODO
  override def compute[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Graph[Double, E] = {
    graph.mapVertices((_, v) => 0.0)
  }
}
