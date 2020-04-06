package com.instagraph.indices.centrality

import com.instagraph.paths.ShortestPaths.AllPairs
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

case class HarmonicCloseness[E: ClassTag](implicit numeric: Numeric[E]) extends CentralityIndex[Double, E] {
  override def compute[V: ClassTag](graph: Graph[V, E]): Graph[Double, E] = {
    graph.allPairsShortestPathsFromDestination.mapVertices { case (origin, spMap) =>
      spMap.filter{ case (destination, _) => destination != origin }
        .mapValues(info => 1.0 / numeric.toDouble(info.totalCost))
        .values
        .sum
    }
  }
}
