package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

case class EigenCentrality[E: ClassTag]() extends CentralityIndex[Double, E] {
  // TODO
  override def compute[V: ClassTag](graph: Graph[V, E]): Graph[Double, E] = {
    graph.mapVertices((_, v) => 0.0)
  }
}
