package com.instagraph.paths

import ShortestPaths._
import com.instagraph.{SparkTest, TestCase}
import com.instagraph.paths.info.EachPathWeightedInfo
import com.instagraph.paths.info.EachFullPathInfo
import com.instagraph.testcases.{TestCase1, TestCase2}
import org.apache.spark.graphx.VertexId
import org.scalatest.flatspec.AnyFlatSpec

class ShortestPathsTest extends AnyFlatSpec with SparkTest with TestCase1 with TestCase2 {
  val testCases: Seq[TestCase[Int]] = Seq(testCase1, testCase2)

  "Shortest paths with successors" should "contain the expected adjacent vertices" in {
    testCases.foreach { test =>
      test.graph.singleSuccessorAPSP.vertices.foreach { case (origin, spMap) =>
        spMap.foreach { case (destination, info) =>
          val solution: EachPathWeightedInfo[Int] = test.adjacentsSolutions(origin)(destination)
          assert(solution.totalCost == info.totalCost)
          if (info.adjacentVertex.isEmpty) assert(solution.adjacentVertices.isEmpty)
          else assert(solution.adjacentVertices.contains(info.adjacentVertex.get))
        }
      }

      test.graph.eachSuccessorAPSP.vertices.foreach { case (origin, spMap) =>
        spMap.foreach { case (destination, info) =>
          val solution: EachPathWeightedInfo[Int] = test.adjacentsSolutions(origin)(destination)
          assert(solution.totalCost == info.totalCost)
          assert(solution.adjacentVertices == info.adjacentVertices)
        }
      }

      test.graph.eachSuccessorWeightedAPSP.vertices.foreach { case (origin, spMap) =>
        spMap.foreach { case (destination, info) =>
          assert(test.adjacentsSolutions(origin)(destination) == info)
        }
      }
    }
  }

  "Full shortest paths from 0" should "be equal to the expected minimum distances" in {
    testCases.foreach { test =>
      test.graph.singleFullPath(0L).vertices.foreach { case (destination, info) =>
        val solution: EachFullPathInfo[Int] = test.fullPathsSolutions(destination)
        assert(solution.cost == info.cost)
        assert(solution.paths.contains(info.path))
      }

      test.graph.eachFullPath(0L).vertices.foreach { case (destination, info) =>
        assert(test.fullPathsSolutions(destination) == info)
      }
    }
  }

  "Shortest paths and fewest hops" should "coincide for unitary edges graphs" in {
    testCases.foreach { test =>
      val numVertices: Int = test.graph.numVertices.toInt

      val hops: List[(VertexId, Map[VertexId, Int])] = test.graph
        .fewestHops(Seq.tabulate(numVertices)(i => i.toLong))
        .vertices
        .toLocalIterator
        .toList

      val sp: List[(VertexId, Map[VertexId, Int])] = test.graph
        .mapEdges(_ => 1)
        .allPairsShortestPaths
        .mapVertices((_, spMap) => spMap.mapValues(info => info.totalCost).map(identity))
        .vertices
        .toLocalIterator
        .toList

      assert(hops == sp)
    }
  }
}