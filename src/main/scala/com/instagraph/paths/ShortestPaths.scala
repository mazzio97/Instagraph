package com.instagraph.paths

import com.instagraph.paths.allpairs.{CostOnly, CostOnlyInfo}
import com.instagraph.paths.allpairs.adjacents.{EachPath, EachPathInfo, EachPathWeighted, EachPathWeightedInfo, SinglePath, SinglePathInfo}
import com.instagraph.paths.allpairs.fullroutes.{EachFullRoute, SingleRoutePath}
import com.instagraph.paths.hops.FewestHops
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

object ShortestPaths {
  implicit class Hops[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = FewestHops(graph).fewestHops(landmarks)
  }

  implicit class AllPairs[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E]) {
    def allPairsShortestPathsFromOrigin: Graph[Map[VertexId, CostOnlyInfo[E]], E] =
      CostOnly(graph).compute

    def allPairsShortestPathsFromDestination: Graph[Map[VertexId, CostOnlyInfo[E]], E] =
      CostOnly(graph, backwardPath = true).compute

    def singleSuccessorAPSP: Graph[Map[VertexId, SinglePathInfo[E]], E] =
      SinglePath(graph).compute

    def eachSuccessorAPSP: Graph[Map[VertexId, EachPathInfo[E]], E] =
      EachPath(graph).compute

    def eachSuccessorWeightedAPSP: Graph[Map[VertexId, EachPathWeightedInfo[E]], E] =
      EachPathWeighted(graph).compute

    def singlePredecessorAPSP: Graph[Map[VertexId, SinglePathInfo[E]], E] =
      SinglePath(graph, backwardPath = true).compute

    def eachPredecessorAPSP: Graph[Map[VertexId, EachPathInfo[E]], E] =
      EachPath(graph, backwardPath = true).compute

    def eachPredecessorWeightedAPSP: Graph[Map[VertexId, EachPathWeightedInfo[E]], E] =
      EachPathWeighted(graph, backwardPath = true).compute

    def singleFullPath: SingleRoutePath[V, E] =
      SingleRoutePath(graph)

    def eachFullPath: EachFullRoute[V, E] =
      EachFullRoute(graph)
  }
}