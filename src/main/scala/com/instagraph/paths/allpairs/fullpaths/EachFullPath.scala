package com.instagraph.paths.allpairs.fullpaths

import com.instagraph.paths.FullPathsInfo
import com.instagraph.paths.ShortestPaths._
import com.instagraph.paths.allpairs.adjacents.EachPathInfo
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class EachFullPath[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E])
  extends FullPathsAllPairShortestPaths[V, E, EachFullPathInfo[E], EachPathInfo[E]](graph.eachPredecessorAPSP) {
  override protected def initializeInfo(cost: E, paths: List[VertexId]*): EachFullPathInfo[E] =
    EachFullPathInfo(cost, paths.toSet)
}

case class EachFullPathInfo[+C](override val cost: C, override val paths: Set[List[VertexId]])
  extends FullPathsInfo[C]