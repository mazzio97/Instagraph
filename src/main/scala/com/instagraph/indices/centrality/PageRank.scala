package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

case class PageRank[E: ClassTag]() extends CentralityIndex[Double, E] {
  override def compute[V: ClassTag](graph: Graph[V, E]): Graph[Double, E] =
    Graph(graph.pageRank(0.0001).vertices, graph.edges)
}
