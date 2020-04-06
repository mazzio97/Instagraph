package com.instagraph.testcases

import com.instagraph.paths.allpairs.adjacents.EachPathWeightedInfo
import com.instagraph.paths.allpairs.fullroutes.EachFullRouteInfo
import com.instagraph.utils.DoubleUtils.TolerantDouble
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class TestCase[E: ClassTag] (
  graph: Graph[Int, E],
  adjacentsSolutions: Map[VertexId, Map[VertexId, EachPathWeightedInfo[E]]],
  fullRoutesSolutions: Map[VertexId, EachFullRouteInfo[E]],
  betweennessCentralitySolutions: Map[VertexId, TolerantDouble],
  degreeCentralitySolutions: Map[VertexId, (Int, Int)],
  harmonicCentralitySolutions: Map[VertexId, TolerantDouble]
)
