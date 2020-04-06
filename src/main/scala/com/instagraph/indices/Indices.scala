package com.instagraph.indices

import com.instagraph.indices.centrality.{BetweennessCentrality, CentralityIndex, HarmonicCloseness, DegreeCentrality, PageRank}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object Indices {
  implicit class Centrality[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def degreeCentrality: Graph[(V, (Int, Int)), E] =
      transformWith(DegreeCentrality())

    def pageRank: Graph[(V, Double), E] =
      transformWith(PageRank())

    def harmonicCloseness(implicit numeric: Numeric[E]): Graph[(V, Double), E] =
      transformWith(HarmonicCloseness())

    def betweennessCentrality(implicit numeric: Numeric[E]): Graph[(V, Double), E] =
      transformWith(BetweennessCentrality())

    private def transformWith[M: ClassTag](index: CentralityIndex[M, E]): Graph[(V, M), E] =
      index.compute(graph).outerJoinVertices(graph.vertices)((_, index, value) => (value.get, index))
  }
}
