package com.instagraph.paths

import com.instagraph.paths.allpairs.adjacents.{CostOnly, EachPath, EachPathWeighted, FromDestination, FromOrigin, SinglePath}
import com.instagraph.paths.allpairs.fullpaths.{EachFullPath, SingleFullPath}
import com.instagraph.paths.hops.FewestHops
import com.instagraph.paths.info.{CostOnlyInfo, EachPathInfo, EachPathWeightedInfo, SinglePathInfo}
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Graph, VertexId}

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

    def singleFullPath: SingleFullPath[V, E] =
      SingleFullPath(graph)

    def eachFullPath: EachFullPath[V, E] =
      EachFullPath(graph)
  }
}