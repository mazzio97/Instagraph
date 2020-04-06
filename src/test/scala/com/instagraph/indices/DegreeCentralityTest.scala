package com.instagraph.indices

import Indices._
import com.instagraph.InstagraphTest
import com.instagraph.testcases.TestCase

class DegreeCentralityTest extends InstagraphTest {
  run { (n: Int, test: TestCase[Int]) =>
    "Degree Centrality computed" should s"be approximately the same of the real one from test case $n" in {
      test.graph.degreeCentrality.vertices.foreach { case (id, (_, degree)) =>
        assert(degree == test.degreeCentralitySolutions(id))
      }
    }
  }
}
