package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * A generic centrality index which can be measured on a graph.
 *
 * @tparam M the vertex type of the output expected by the index
 * @tparam E the edge type of the graph
 */
trait CentralityIndex[M, E] {
  def compute[V: ClassTag](graph: Graph[V, E]): Graph[M, E]
}