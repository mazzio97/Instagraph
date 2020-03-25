package com.instagraph.paths

import ShortestPaths.Distances
import ShortestPaths.Hops
import com.instagraph.SparkTest
import org.apache.spark.graphx.{Edge, Graph}
import org.scalatest.flatspec.AnyFlatSpec

class ShortestPathsTest extends AnyFlatSpec with SparkTest {

  private val vertices = sparkContext.makeRDD(Array(
    (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G"))
  )

  private val edges = sparkContext.makeRDD(Array(
    Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0), Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0),
    Edge(2L, 5L, 7.0), Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
    Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0))
  )

  private val graph = Graph(vertices, edges)

  "Shortest paths from A" should "be equal to the expected minimum distances" in {
    val solution = Array(
      ("D", (5.0, List(1L))), ("A", (0.0, List())), ("F", (11.0, List(1L, 4L))), ("C", (15.0, List(1L, 2L))),
      ("G", (22.0, List(1L, 4L, 6L))), ("E", (14.0, List(1L, 2L))), ("B", (7.0, List(1L)))
    )
    graph.dijkstra(1L).vertices.foreach(v => assert(solution.contains(v._2)))
  }

  "Dijkstra and fewest hops" should "coincide for unitary edges graphs" in {
    val unitaryGraph: Graph[String, Double] = Graph(vertices, edges.map(e => Edge(e.srcId, e.dstId, 1.0)))
    val hopsVertices = unitaryGraph.fewestHops(1L to 7L).vertices

    for (origin <- 1L to 7L) {
      val originMap = hopsVertices.filter(v => v._1 == origin).first()._2
      unitaryGraph.dijkstra(origin).vertices.map(v => (v._1, v._2._2._1)).foreach(v =>
        assert(originMap.getOrElse(v._1, Double.PositiveInfinity) == v._2)
      )
    }
  }
}
