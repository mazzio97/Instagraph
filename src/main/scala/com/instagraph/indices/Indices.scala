package com.instagraph.indices

import com.instagraph.indices.centrality.{BetweennessCentrality, CentralityIndex, ClosenessCentrality, DegreeCentrality, EigenCentrality, PageRank}
import com.instagraph.utils.GraphUtils.Manipulations

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object Indices {
  implicit class Centrality[V: ClassTag](graph: Graph[V, Double]) {
    def pageRank: Graph[(V, Double), Double] = graph transformWith PageRank

    def degreeCentrality: Graph[(V, (Int, Int)), Double] = graph transformWith DegreeCentrality

    def closeCentrality: Graph[(V, Double), Double] = graph transformWith ClosenessCentrality

    def betweennessCentrality: Graph[(V, Double), Double] = graph transformWith BetweennessCentrality

    def eigenCentrality: Graph[(V, Double), Double] = graph transformWith EigenCentrality
  }
}
