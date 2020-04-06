package com.instagraph.paths

import ShortestPaths._
import com.instagraph.InstagraphTest
import com.instagraph.paths.allpairs.fullroutes.EachFullRouteInfo
import com.instagraph.testcases.TestCase

class FullRoutesTest extends InstagraphTest {
  runTests { (n: Int, test: TestCase[Int]) =>
    "A single shortest path from 0" should s"be contained into the expected routes from test case $n" in {
      test.graph.singleFullPath(0L).vertices.foreach { case (destination, info) =>
        val solution: EachFullRouteInfo[Int] = test.fullRoutesSolutions(destination)
        assert(solution.totalCost == info.totalCost)
        assert(solution.paths.contains(info.path))
      }
    }

    "The set of each shortest path from 0" should s"be equal to the expected routes from test case $n" in {
      test.graph.eachFullPath(0L).vertices.foreach { case (destination, info) =>
        assert(test.fullRoutesSolutions(destination) == info)
      }
    }
  }
}
