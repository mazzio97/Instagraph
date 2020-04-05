package com.instagraph.paths.allpairs.fullroutes

import com.instagraph.paths.ShortestPaths._
import com.instagraph.paths.allpairs.adjacents.EachPathInfo
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class EachFullRoute[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E])
  extends FullRoutesAllPairShortestPaths[V, E, EachFullRouteInfo[E], EachPathInfo[E]](graph.eachPredecessorAPSP) {
  override protected def initializeInfo(cost: E, paths: List[VertexId]*): EachFullRouteInfo[E] =
    EachFullRouteInfo(cost, paths.toSet)
}