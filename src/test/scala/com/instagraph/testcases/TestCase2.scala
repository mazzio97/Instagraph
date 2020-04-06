package com.instagraph.testcases

import com.instagraph.SparkConfiguration._
import com.instagraph.paths.allpairs.adjacents.EachPathWeightedInfo
import com.instagraph.paths.allpairs.fullroutes.EachFullRouteInfo
import org.apache.spark.graphx.Edge

object TestCase2 extends TestCase[Int](
  graph = createGraph(sparkContext.makeRDD(
    Seq((0, 1), (1, 2), (1, 3), (2, 4), (3, 4), (4, 5), (5, 6), (5, 10), (6, 7), (7, 8), (8, 9), (9, 10))
      .map { case (source, destination) => Edge(source.toLong, destination.toLong, destination - source) }
      ++ Seq(Edge(6L, 11L, 1), Edge(11L, 9L, 2))
  )),
  adjacentsSolutions = Map(
    0L -> Map(
      0L -> EachPathWeightedInfo(0, Map.empty),
      1L -> EachPathWeightedInfo(1, Map(1L -> 1)),
      2L -> EachPathWeightedInfo(2, Map(1L -> 1)),
      3L -> EachPathWeightedInfo(3, Map(1L -> 1)),
      4L -> EachPathWeightedInfo(4, Map(1L -> 2)),
      5L -> EachPathWeightedInfo(5, Map(1L -> 2)),
      6L -> EachPathWeightedInfo(6, Map(1L -> 2)),
      7L -> EachPathWeightedInfo(7, Map(1L -> 2)),
      8L -> EachPathWeightedInfo(8, Map(1L -> 2)),
      9L -> EachPathWeightedInfo(9, Map(1L -> 4)),
      10L -> EachPathWeightedInfo(10, Map(1L -> 6)),
      11L -> EachPathWeightedInfo(7, Map(1L -> 2))
    ),
    1L -> Map(
      1L -> EachPathWeightedInfo(0, Map.empty),
      2L -> EachPathWeightedInfo(1, Map(2L -> 1)),
      3L -> EachPathWeightedInfo(2, Map(3L -> 1)),
      4L -> EachPathWeightedInfo(3, Map(3L -> 1, 2L -> 1)),
      5L -> EachPathWeightedInfo(4, Map(3L -> 1, 2L -> 1)),
      6L -> EachPathWeightedInfo(5, Map(3L -> 1, 2L -> 1)),
      7L -> EachPathWeightedInfo(6, Map(3L -> 1, 2L -> 1)),
      8L -> EachPathWeightedInfo(7, Map(3L -> 1, 2L -> 1)),
      9L -> EachPathWeightedInfo(8, Map(3L -> 2, 2L -> 2)),
      10L -> EachPathWeightedInfo(9, Map(3L -> 3, 2L -> 3)),
      11L -> EachPathWeightedInfo(6, Map(3L -> 1, 2L -> 1))
    ),
    2L -> Map(
      2L -> EachPathWeightedInfo(0, Map.empty),
      4L -> EachPathWeightedInfo(2, Map(4L -> 1)),
      5L -> EachPathWeightedInfo(3, Map(4L -> 1)),
      6L -> EachPathWeightedInfo(4, Map(4L -> 1)),
      7L -> EachPathWeightedInfo(5, Map(4L -> 1)),
      8L -> EachPathWeightedInfo(6, Map(4L -> 1)),
      9L -> EachPathWeightedInfo(7, Map(4L -> 2)),
      10L -> EachPathWeightedInfo(8, Map(4L -> 3)),
      11L -> EachPathWeightedInfo(5, Map(4L -> 1))
    ),
    3L -> Map(
      3L -> EachPathWeightedInfo(0, Map.empty),
      4L -> EachPathWeightedInfo(1, Map(4L -> 1)),
      5L -> EachPathWeightedInfo(2, Map(4L -> 1)),
      6L -> EachPathWeightedInfo(3, Map(4L -> 1)),
      7L -> EachPathWeightedInfo(4, Map(4L -> 1)),
      8L -> EachPathWeightedInfo(5, Map(4L -> 1)),
      9L -> EachPathWeightedInfo(6, Map(4L -> 2)),
      10L -> EachPathWeightedInfo(7, Map(4L -> 3)),
      11L -> EachPathWeightedInfo(4, Map(4L -> 1))
    ),
    4L -> Map(
      4L -> EachPathWeightedInfo(0, Map.empty),
      5L -> EachPathWeightedInfo(1, Map(5L -> 1)),
      6L -> EachPathWeightedInfo(2, Map(5L -> 1)),
      7L -> EachPathWeightedInfo(3, Map(5L -> 1)),
      8L -> EachPathWeightedInfo(4, Map(5L -> 1)),
      9L -> EachPathWeightedInfo(5, Map(5L -> 2)),
      10L -> EachPathWeightedInfo(6, Map(5L -> 3)),
      11L -> EachPathWeightedInfo(3, Map(5L -> 1))
    ),
    5L -> Map(
      5L -> EachPathWeightedInfo(0, Map.empty),
      6L -> EachPathWeightedInfo(1, Map(6L -> 1)),
      7L -> EachPathWeightedInfo(2, Map(6L -> 1)),
      8L -> EachPathWeightedInfo(3, Map(6L -> 1)),
      9L -> EachPathWeightedInfo(4, Map(6L -> 2)),
      10L -> EachPathWeightedInfo(5, Map(10L -> 1, 6L -> 2)),
      11L -> EachPathWeightedInfo(2, Map(6L -> 1))
    ),
    6L -> Map(
      6L -> EachPathWeightedInfo(0, Map.empty),
      7L -> EachPathWeightedInfo(1, Map(7L -> 1)),
      8L -> EachPathWeightedInfo(2, Map(7L -> 1)),
      9L -> EachPathWeightedInfo(3, Map(11L -> 1, 7L -> 1)),
      10L -> EachPathWeightedInfo(4, Map(11L -> 1, 7L -> 1)),
      11L -> EachPathWeightedInfo(1, Map(11L -> 1))
    ),
    7L -> Map(
      7L -> EachPathWeightedInfo(0, Map.empty),
      8L -> EachPathWeightedInfo(1, Map(8L -> 1)),
      9L -> EachPathWeightedInfo(2, Map(8L -> 1)),
      10L -> EachPathWeightedInfo(3, Map(8L -> 1))
    ),
    8L -> Map(
      8L -> EachPathWeightedInfo(0, Map.empty),
      9L -> EachPathWeightedInfo(1, Map(9L -> 1)),
      10L -> EachPathWeightedInfo(2, Map(9L -> 1))
    ),
    9L -> Map(
      9L -> EachPathWeightedInfo(0, Map.empty),
      10L -> EachPathWeightedInfo(1, Map(10L -> 1))
    ),
    10L -> Map(
      10L -> EachPathWeightedInfo(0, Map.empty)
    ),
    11L -> Map(
      9L -> EachPathWeightedInfo(2, Map(9L -> 1)),
      10L -> EachPathWeightedInfo(3, Map(9L -> 1)),
      11L -> EachPathWeightedInfo(0, Map.empty)
    )
  ),
  fullRoutesSolutions = Map(
    0L -> EachFullRouteInfo(0, Set(List(0L))),
    1L -> EachFullRouteInfo(1, Set(List(0L, 1L))),
    2L -> EachFullRouteInfo(2, Set(List(0L, 1L, 2L))),
    3L -> EachFullRouteInfo(3, Set(List(0L, 1L, 3L))),
    4L -> EachFullRouteInfo(4, Set(List(0L, 1L, 3L, 4L), List(0L, 1L, 2L, 4L))),
    5L -> EachFullRouteInfo(5, Set(List(0L, 1L, 3L, 4L, 5L), List(0L, 1L, 2L, 4L, 5L))),
    6L -> EachFullRouteInfo(6, Set(List(0L, 1L, 3L, 4L, 5L, 6L), List(0L, 1L, 2L, 4L, 5L, 6L))),
    7L -> EachFullRouteInfo(7, Set(List(0L, 1L, 3L, 4L, 5L, 6L, 7L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L))),
    8L -> EachFullRouteInfo(8, Set(List(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L, 8L))),
    9L -> EachFullRouteInfo(9, Set(
      List(0L, 1L, 3L, 4L, 5L, 6L, 11L, 9L), List(0L, 1L, 2L, 4L, 5L, 6L, 11L, 9L),
      List(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L)
    )),
    10L -> EachFullRouteInfo(10, Set(
      List(0L, 1L, 3L, 4L, 5L, 6L, 11L, 9L, 10L), List(0L, 1L, 3L, 4L, 5L, 10L),
      List(0L, 1L, 2L, 4L, 5L, 6L, 11L, 9L, 10L), List(0L, 1L, 2L, 4L, 5L, 10L),
      List(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
    )),
    11L -> EachFullRouteInfo(7, Set(List(0L, 1L, 3L, 4L, 5L, 6L, 11L), List(0L, 1L, 2L, 4L, 5L, 6L, 11L)))
  ),
  betweennessCentralitySolutions = Map(
    0L -> 0.0,
    1L -> 10.0,
    2L -> 8.0,
    3L -> 8.0,
    4L -> 28.0,
    5L -> 30.0,
    6L -> 28.0,
    7L -> 13.0,
    8L -> 8.0,
    9L -> 8.0,
    10L -> 0.0,
    11L -> 6.0
  ),
  degreeCentralitySolutions = Map(
    0L -> (0, 1),
    1L -> (1, 2),
    2L -> (1, 1),
    3L -> (1, 1),
    4L -> (2, 1),
    5L -> (1, 2),
    6L -> (1, 2),
    7L -> (1, 1),
    8L -> (1, 1),
    9L -> (2, 1),
    10L -> (2, 0),
    11L -> (1, 1)
  ),
  harmonicCentralitySolutions = Map(
    0L -> 0,
    1L -> 1.0,
    2L -> 1.5,
    3L -> 0.8333333333333333,
    4L -> 2.083333333333333,
    5L -> 2.283333333333333,
    6L -> 2.45,
    7L -> 2.5928571428571425,
    8L -> 2.7178571428571425,
    9L -> 3.328968253968254,
    10L -> 3.2623015873015873,
    11L -> 2.5928571428571425
  )
)
