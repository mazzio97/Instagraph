package com.instagraph.paths.allpairs.fullroutes

import com.instagraph.paths.ShortestPaths._
import com.instagraph.paths.allpairs.adjacents.SinglePathInfo
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class SingleRoutePath[V: ClassTag, E: ClassTag](graph: Graph[V, E])(implicit numeric: Numeric[E])
  extends FullRoutesAllPairShortestPaths[V, E, SingleFullRouteInfo[E], SinglePathInfo[E]](graph.singlePredecessorAPSP) {

  override protected def initializeInfo(cost: E, paths: List[VertexId]*): SingleFullRouteInfo[E] =
    SingleFullRouteInfo(cost, paths.head)
}