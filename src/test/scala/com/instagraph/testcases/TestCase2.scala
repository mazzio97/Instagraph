package com.instagraph.testcases

import com.instagraph.paths.allpairs.adjacents.EachPathWeightedInfo
import com.instagraph.{SparkTest, TestCase}
import com.instagraph.paths.allpairs.fullpaths.EachFullPathInfo
import org.apache.spark.graphx.Edge

trait TestCase2 extends SparkTest {
  val testCase2: TestCase[Int] = TestCase[Int](
    graph = createGraph(sparkContext.makeRDD(
      Seq((0, 1), (1, 2), (1, 3), (2, 4), (3, 4), (4, 5), (5, 6), (5, 10), (6, 7), (7, 8), (8, 9), (9, 10))
        .map { case (source, destination) => Edge(source.toLong, destination.toLong, destination - source) }
        ++ Seq(Edge(6L, 11L, 1), Edge(11L, 9L, 2))
    )),
    fullPathsSolutions = Map(
      0L -> EachFullPathInfo(0, Set(List(0L))),
      1L -> EachFullPathInfo(1, Set(List(0L, 1L))),
      2L -> EachFullPathInfo(2, Set(List(0L, 1L, 2L))),
      3L -> EachFullPathInfo(3, Set(List(0L, 1L, 3L))),
      4L -> EachFullPathInfo(4, Set(List(0L, 1L, 3L, 4L), List(0L, 1L, 2L, 4L))),
      5L -> EachFullPathInfo(5, Set(List(0L, 1L, 3L, 4L, 5L), List(0L, 1L, 2L, 4L, 5L))),
      6L -> EachFullPathInfo(6, Set(List(0L, 1L, 3L, 4L, 5L, 6L), List(0L, 1L, 2L, 4L, 5L, 6L))),
      7L -> EachFullPathInfo(7, Set(List(0L, 1L, 3L, 4L, 5L, 6L, 7L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L))),
      8L -> EachFullPathInfo(8, Set(List(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L, 8L))),
      9L -> EachFullPathInfo(9, Set(
        List(0L, 1L, 3L, 4L, 5L, 6L, 11L, 9L), List(0L, 1L, 2L, 4L, 5L, 6L, 11L, 9L),
        List(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L)
      )),
      10L -> EachFullPathInfo(10, Set(
        List(0L, 1L, 3L, 4L, 5L, 6L, 11L, 9L, 10L), List(0L, 1L, 3L, 4L, 5L, 10L),
        List(0L, 1L, 2L, 4L, 5L, 6L, 11L, 9L, 10L), List(0L, 1L, 2L, 4L, 5L, 10L),
        List(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), List(0L, 1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
      )),
      11L -> EachFullPathInfo(7, Set(List(0L, 1L, 3L, 4L, 5L, 6L, 11L), List(0L, 1L, 2L, 4L, 5L, 6L, 11L)))
    ),
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
    )
  )
}
