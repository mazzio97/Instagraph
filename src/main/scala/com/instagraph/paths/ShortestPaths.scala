package com.instagraph.paths

import com.instagraph.paths.allpairs.{CostOnly, CostOnlyInfo}
import com.instagraph.paths.allpairs.adjacents.{EachPath, EachPathInfo, EachPathWeighted, EachPathWeightedInfo, SinglePath, SinglePathInfo}
import com.instagraph.paths.allpairs.fullpaths.{EachFullPath, SingleFullPath}
import com.instagraph.paths.hops.FewestHops
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

object ShortestPaths {
  implicit class Hops[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = FewestHops(graph).fewestHops(landmarks)
  }

  implicit class AllPairs[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E]) {
    def allPairsShortestPaths: Graph[Map[VertexId, CostOnlyInfo[E]], E] =
      CostOnly(graph).computeAPSP

    def singleSuccessorAPSP: Graph[Map[VertexId, SinglePathInfo[E]], E] =
      SinglePath(graph).computeAPSP

    def eachSuccessorAPSP: Graph[Map[VertexId, EachPathInfo[E]], E] =
      EachPath(graph).computeAPSP

    def eachSuccessorWeightedAPSP: Graph[Map[VertexId, EachPathWeightedInfo[E]], E] =
      EachPathWeighted(graph).computeAPSP

    def singlePredecessorAPSP: Graph[Map[VertexId, SinglePathInfo[E]], E] =
      SinglePath(graph, backwardPath = true).computeAPSP

    def eachPredecessorAPSP: Graph[Map[VertexId, EachPathInfo[E]], E] =
      EachPath(graph, backwardPath = true).computeAPSP

    def eachPredecessorWeightedAPSP: Graph[Map[VertexId, EachPathWeightedInfo[E]], E] =
      EachPathWeighted(graph, backwardPath = true).computeAPSP

    def singleFullPath: SingleFullPath[V, E] =
      SingleFullPath(graph)

    def eachFullPath: EachFullPath[V, E] =
      EachFullPath(graph)
  }
}