package com.instagraph.indices.centrality

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object DegreeCentrality extends CentralityIndex[(Int, Int)] {
  override def compute[V: ClassTag, E: ClassTag](graph: Graph[V, E]): Graph[(Int, Int), E] = {
    graph.outerJoinVertices[Int, Int](graph.inDegrees)((id, v, in) =>
      in.getOrElse(0)
    ).outerJoinVertices[Int, (Int, Int)](graph.outDegrees)((id, in, out) =>
      (in, out.getOrElse(0))
    )
  }
}
