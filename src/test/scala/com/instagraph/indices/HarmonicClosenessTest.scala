package com.instagraph.indices

import Indices._
import com.instagraph.InstagraphTest
import com.instagraph.testcases.TestCase

case class HarmonicClosenessTest() extends InstagraphTest {
  run { (n: Int, test: TestCase[Int]) =>
    "Harmonic Centrality computed" should s"be approximately the same of the real one from test case $n" in {
      test.graph.harmonicCloseness.vertices.foreach { case (id, (_, value)) =>
        assert(test.harmonicCentralitySolutions(id) == value)
      }
    }
  }
}
