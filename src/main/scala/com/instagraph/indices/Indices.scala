package com.instagraph.indices

import com.instagraph.indices.centrality.{BetweennessCentrality, CentralityIndex, ClosenessCentrality, DegreeCentrality, PageRank}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object Indices {
  implicit class Indices[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def degreeCentrality: Graph[(V, (Int, Int)), E] = transformWith(DegreeCentrality())

    def betweennessCentrality(implicit numeric: Numeric[E]): Graph[(V, Double), E] = transformWith(BetweennessCentrality())

    def pageRank: Graph[(V, Double), E] = transformWith(PageRank())

    def closeCentrality: Graph[(V, Double), E] = transformWith(ClosenessCentrality())

    private def transformWith[M: ClassTag](index: CentralityIndex[M, E]): Graph[(V, M), E] =
      index.compute(graph).outerJoinVertices(graph.vertices)((_, index, value) => (value.get, index))
  }
}
