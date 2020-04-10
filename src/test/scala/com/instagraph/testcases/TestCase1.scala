package com.instagraph.testcases

import com.instagraph.utils.SparkConfiguration._
import com.instagraph.paths.allpairs.adjacents.EachPathWeightedInfo
import com.instagraph.paths.allpairs.fullroutes.EachFullRouteInfo
import com.instagraph.utils.DoubleUtils.TolerantDouble
import org.apache.spark.graphx.Edge

object TestCase1 extends TestCase[Int](
  graph = createGraph(sparkContext.makeRDD(
    Seq((0, 1), (0, 2), (1, 2), (1, 3), (2, 3), (3, 4))
      .map { case (source, destination) => Edge(source.toLong, destination.toLong, (destination - source)) }
      ++ Seq(Edge(4L, 0L, 1))
  )),
  adjacentsSolutions = Map(
    0L -> Map(
      0L -> EachPathWeightedInfo(0, Map.empty),
      1L -> EachPathWeightedInfo(1, Map(1L -> 1)),
      2L -> EachPathWeightedInfo(2, Map(2L -> 1, 1L -> 1)),
      3L -> EachPathWeightedInfo(3, Map(2L -> 1, 1L -> 2)),
      4L -> EachPathWeightedInfo(4, Map(2L -> 1, 1L -> 2))
    ),
    1L -> Map(
      0L -> EachPathWeightedInfo(4, Map(3L -> 1, 2L -> 1)),
      1L -> EachPathWeightedInfo(0, Map.empty),
      2L -> EachPathWeightedInfo(1, Map(2L -> 1)),
      3L -> EachPathWeightedInfo(2, Map(3L -> 1, 2L -> 1)),
      4L -> EachPathWeightedInfo(3, Map(3L -> 1, 2L -> 1))
    ),
    2L -> Map(
      0L -> EachPathWeightedInfo(3, Map(3L -> 1)),
      1L -> EachPathWeightedInfo(4, Map(3L -> 1)),
      2L -> EachPathWeightedInfo(0, Map.empty),
      3L -> EachPathWeightedInfo(1, Map(3L -> 1)),
      4L -> EachPathWeightedInfo(2, Map(3L -> 1))
    ),
    3L -> Map(
      0L -> EachPathWeightedInfo(2, Map(4L -> 1)),
      1L -> EachPathWeightedInfo(3, Map(4L -> 1)),
      2L -> EachPathWeightedInfo(4, Map(4L -> 2)),
      3L -> EachPathWeightedInfo(0, Map.empty),
      4L -> EachPathWeightedInfo(1, Map(4L -> 1))
    ),
    4L -> Map(
      0L -> EachPathWeightedInfo(1, Map(0L -> 1)),
      1L -> EachPathWeightedInfo(2, Map(0L -> 1)),
      2L -> EachPathWeightedInfo(3, Map(0L -> 2)),
      3L -> EachPathWeightedInfo(4, Map(0L -> 3)),
      4L -> EachPathWeightedInfo(0, Map.empty)
    )),
  fullRoutesSolutions = Map(
    0L -> EachFullRouteInfo(0, Set(List(0L))),
    1L -> EachFullRouteInfo(1, Set(List(0L, 1L))),
    2L -> EachFullRouteInfo(2, Set(List(0L, 2L), List(0L, 1L, 2L))),
    3L -> EachFullRouteInfo(3, Set(List(0L, 1L, 3L), List(0L, 2L, 3L), List(0L, 1L, 2L, 3L))),
    4L -> EachFullRouteInfo(4, Set(List(0L, 1L, 3L, 4L), List(0L, 2L, 3L, 4L), List(0L, 1L, 2L, 3L, 4L)))
  ),
  degreeCentralitySolutions = Map(
    0L -> (1, 2),
    1L -> (1, 2),
    2L -> (2, 1),
    3L -> (2, 1),
    4L -> (1, 1)
  ),
  pageRankSolutions = Map(
    0L -> TolerantDouble(1.1319885118034128),
    1L -> TolerantDouble(0.6311596882000914),
    2L -> TolerantDouble(0.8992721773930266),
    3L -> TolerantDouble(1.1824199222270388),
    4L -> TolerantDouble(1.15515970037643)
  ),
  harmonicCentralitySolutions = Map(
    0L -> TolerantDouble(2.083333333333333),
    1L -> TolerantDouble(2.083333333333333),
    2L -> TolerantDouble(2.083333333333333),
    3L -> TolerantDouble(2.083333333333333),
    4L -> TolerantDouble(2.083333333333333)
  ),
  betweennessCentralitySolutions = Map(
    0L -> TolerantDouble(6.0),
    1L -> TolerantDouble(3.5),
    2L -> TolerantDouble(3.5),
    3L -> TolerantDouble(6.0),
    4L -> TolerantDouble(6.0)
  )
)
