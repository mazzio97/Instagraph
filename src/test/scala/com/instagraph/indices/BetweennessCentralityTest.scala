package com.instagraph.indices

import Indices._
import com.instagraph.InstagraphTest
import com.instagraph.testcases.TestCase

class BetweennessCentralityTest extends InstagraphTest {
  run { (n: Int, test: TestCase[Int]) =>
    "Betweenness Centrality computed" should s"be approximately the same of the real one from test case $n" in {
      test.graph.betweennessCentrality.vertices.foreach { case (id, (_, value)) =>
        assert(test.betweennessCentralitySolutions(id) == value)
      }
    }
  }
}