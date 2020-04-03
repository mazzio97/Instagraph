package com.instagraph.paths

import com.instagraph.paths.allpairs.adjacents.{CostOnly, CostOnlyInfo, EachPath, EachPathInfo, EachPathWeighted, EachPathWeightedInfo, FromDestination, FromOrigin, SinglePath, SinglePathInfo}
import com.instagraph.paths.hops.FewestHops
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, lib}

import scala.reflect.ClassTag

object ShortestPaths {
  implicit class Hops[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = FewestHops(graph).fewestHops(landmarks)
  }

  implicit class AllPairs[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E]) {
    def allPairsShortestPaths: Graph[Map[VertexId, CostOnlyInfo[E]], E] =
      CostOnly(graph, direction = FromOrigin).computeAPSP

    def singleSuccessorAPSP: Graph[Map[VertexId, SinglePathInfo[E]], E] =
      SinglePath(graph, direction = FromOrigin).computeAPSP

    def eachSuccessorAPSP: Graph[Map[VertexId, EachPathInfo[E]], E] =
      EachPath(graph, direction = FromOrigin).computeAPSP

    def eachSuccessorWeightedAPSP: Graph[Map[VertexId, EachPathWeightedInfo[E]], E] =
      EachPathWeighted(graph, direction = FromOrigin).computeAPSP

    def singlePredecessorAPSP: Graph[Map[VertexId, SinglePathInfo[E]], E] =
      SinglePath(graph, direction = FromDestination).computeAPSP

    def eachPredecessorAPSP: Graph[Map[VertexId, EachPathInfo[E]], E] =
      EachPath(graph, direction = FromDestination).computeAPSP

    def eachPredecessorWeightedAPSP: Graph[Map[VertexId, EachPathWeightedInfo[E]], E] =
      EachPathWeighted(graph, direction = FromDestination).computeAPSP
  }
}

/**
 * Data structure to represent info about the cost of the shortest paths between a pair of vertices
 *
 * @tparam C the edge/cost type
 */
trait ShortestPathsInfo[+C] {
  val totalCost: C
}

/**
 * Data structure to represent info about the cost and the actual paths of the shortest paths between a pair of vertices
 *
 * @tparam C the type of edge/cost
 */
trait ShortestPathsWithAdjacentVerticesInfo[+C] extends ShortestPathsInfo[C] {
  def adjacentVertices: Set[VertexId]
}
