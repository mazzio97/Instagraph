package com.instagraph.paths

import com.instagraph.paths.allpairs.{EachSuccessor, EachPathInfo, EachSuccessorWeighted, EachPathWeightedInfo, FromOrigin, SingleSuccessor, SingleSuccessorInfo}
import com.instagraph.paths.hops.FewestHops
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {
  implicit class Hops[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = FewestHops(graph).fewestHops(landmarks)
  }

  implicit class AllPairs[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E]) {
    def singleSuccessorAPSP: Graph[Map[VertexId, SingleSuccessorInfo[E]], E] =
      SingleSuccessor(graph, direction = FromOrigin).computeAPSP

    def eachSuccessorAPSP: Graph[Map[VertexId, EachPathInfo[E]], E] =
      EachSuccessor(graph, direction = FromOrigin).computeAPSP

    def eachSuccessorWeightedAPSP: Graph[Map[VertexId, EachPathWeightedInfo[E]], E] =
      EachSuccessorWeighted(graph, direction = FromOrigin).computeAPSP
  }
}

/**
 * Data structure to represent info about a shortest path.
 *
 * @tparam C the edge/cost type
 */
trait ShortestPathsInfo[+C] {
  val totalCost: C
}
