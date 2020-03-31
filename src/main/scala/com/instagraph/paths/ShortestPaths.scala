package com.instagraph.paths

import com.instagraph.paths.allpairs.EachSuccessorWeighted
import com.instagraph.paths.hops.FewestHops
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {
  implicit def fewestHops[V: ClassTag, E: ClassTag](graph: Graph[V, E]): FewestHops[V, E] =
    new FewestHops(graph)

  implicit def allPairsShortestPath[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E]): EachSuccessorWeighted[V, E] =
    EachSuccessorWeighted(graph)
}

/**
 * Data structure to represent info about a shortest path.
 *
 * @tparam C the edge/cost type
 */
trait ShortestPathsInfo[+C] {
  val totalCost: C
}
