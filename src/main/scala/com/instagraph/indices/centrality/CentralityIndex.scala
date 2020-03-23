package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * A generic centrality index which can be measured on a graph.
 *
 * @tparam M the type of the output expected by the index
 */
trait CentralityIndex[M] {
  def compute[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Graph[M, E]
}
