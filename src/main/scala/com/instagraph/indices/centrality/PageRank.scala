package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object PageRank extends CentralityIndex[Double] {
  override def compute[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Graph[Double, E] = { Graph(
    graph.pageRank(0.0001).vertices,
    graph.edges
  ) }
}
