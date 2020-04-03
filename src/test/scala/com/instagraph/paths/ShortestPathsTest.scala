package com.instagraph.paths

import ShortestPaths._
import ShortestPathsUtils.Manipulations
import com.instagraph.SparkTest
import com.instagraph.paths.allpairs.adjacents.SinglePathInfo
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.scalatest.flatspec.AnyFlatSpec

class ShortestPathsTest extends AnyFlatSpec with SparkTest {

  private val numVertices: Int = 5
  private val vertices: RDD[(Long, Int)] = sparkContext.makeRDD(Seq.tabulate(numVertices)(i => (i.toLong, i)))
  private val edges: RDD[Edge[Int]] = sparkContext.makeRDD(
    Seq(Edge(0L, 1L, 1), Edge(0L, 2L, 1), Edge(1L, 2L, 0), Edge(1L, 3L, 1), Edge(2L, 3L, 1), Edge(3L, 4L, 1))
  )
  private val graph: Graph[Int, Int] = Graph(vertices, edges)

  "Shortest paths from 0" should "be equal to the expected minimum distances" in {
    val solution: Map[VertexId, (Int, Set[List[VertexId]])] = Map(
      0L -> (0, Set(List(0L))),
      1L -> (1, Set(List(0L, 1L))),
      2L -> (1, Set(List(0L, 2L), List(0L, 1L, 2L))),
      3L -> (2, Set(List(0L, 1L, 3L), List(0L, 2L, 3L), List(0L, 1L, 2L, 3L))),
      4L -> (3, Set(List(0L, 1L, 3L, 4L), List(0L, 2L, 3L, 4L), List(0L, 1L, 2L, 3L, 4L)))
    )

    assert(graph.eachSuccessorWeightedAPSP.shortestPathsFrom(0L).vertices.collectAsMap() == solution)
  }

  "Shortest paths and fewest hops" should "coincide for unitary edges graphs" in {
    val hopsVertices: Set[(VertexId, VertexId, Int)] = graph.fewestHops(Seq.tabulate(numVertices)(i => i.toLong))
      .vertices
      .collectAsMap()
      .flatMap { case (origin, spMap) => spMap.map { case (destination, cost) => (origin, destination, cost) } }
      .toSet

    val spVertices: Set[(VertexId, VertexId, Int)] = graph.mapEdges(_ => 1).allPairsShortestPaths
      .vertices
      .collectAsMap()
      .flatMap { case (origin, spMap) => spMap.map { case (destination, info) => (origin, destination, info.totalCost) } }
      .toSet

    assert(spVertices == hopsVertices)
  }
}
