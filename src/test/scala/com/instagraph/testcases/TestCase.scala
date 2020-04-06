package com.instagraph.testcases

import com.instagraph.paths.allpairs.adjacents.EachPathWeightedInfo
import com.instagraph.paths.allpairs.fullroutes.EachFullRouteInfo
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class TestCase[E: ClassTag] (
  graph: Graph[Int, E],
  adjacentsSolutions: Map[VertexId, Map[VertexId, EachPathWeightedInfo[E]]],
  fullRoutesSolutions: Map[VertexId, EachFullRouteInfo[E]]
)
