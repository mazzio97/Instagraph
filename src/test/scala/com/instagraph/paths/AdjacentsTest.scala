package com.instagraph.paths

import ShortestPaths._
import com.instagraph.InstagraphTest
import com.instagraph.paths.allpairs.adjacents.EachPathWeightedInfo
import com.instagraph.testcases.TestCase

class AdjacentsTest extends InstagraphTest {
  runTests { (n: Int, test: TestCase[Int]) =>
    "Successors from singleSuccessorsAPSP" should s"be contained into the set of adjacent vertices from test case $n" in {
      test.graph.singleSuccessorAPSP.vertices.foreach { case (origin, spMap) =>
        spMap.foreach { case (destination, info) =>
          val solution: EachPathWeightedInfo[Int] = test.adjacentsSolutions(origin)(destination)
          assert(solution.totalCost == info.totalCost)
          if (info.adjacentVertex.isEmpty) assert(solution.adjacentVertices.isEmpty)
          else assert(solution.adjacentVertices.contains(info.adjacentVertex.get))
        }
      }
    }

    "Successors from eachSuccessorsAPSP" should s"be the same of the adjacent vertices from test case $n" in
      test.graph.eachSuccessorAPSP.vertices.foreach { case (origin, spMap) =>
        spMap.foreach { case (destination, info) =>
          val solution: EachPathWeightedInfo[Int] = test.adjacentsSolutions(origin)(destination)
          assert(solution.totalCost == info.totalCost)
          assert(solution.adjacentVertices == info.adjacentVertices)
        }
      }

    "Both successors and weights from eachSuccessorsWeightedAPSP" should s"be the same from test case $n" in {
      test.graph.eachSuccessorWeightedAPSP.vertices.foreach { case (origin, spMap) =>
        spMap.foreach { case (destination, info) =>
          assert(test.adjacentsSolutions(origin)(destination) == info)
        }
      }
    }
  }
}