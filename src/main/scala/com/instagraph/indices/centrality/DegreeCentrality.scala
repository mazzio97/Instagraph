package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

case class DegreeCentrality[E: ClassTag]() extends CentralityIndex[(Int, Int), E] {
  override def compute[V: ClassTag](graph: Graph[V, E]): Graph[(Int, Int), E] = {
    graph.outerJoinVertices[Int, Int](graph.inDegrees)((_, _, in) =>
      in.getOrElse(0)
    ).outerJoinVertices[Int, (Int, Int)](graph.outDegrees)((_, in, out) =>
      (in, out.getOrElse(0))
    )
  }
}
