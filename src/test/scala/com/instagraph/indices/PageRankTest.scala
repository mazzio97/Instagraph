package com.instagraph.indices

import Indices._
import com.instagraph.InstagraphTest
import com.instagraph.testcases.TestCase

class PageRankTest extends InstagraphTest {
  run { (n: Int, test: TestCase[Int]) =>
    "Page Rank computed" should s"be approximately the same of the real one from test case $n" in {
      test.graph.pageRank.vertices.foreach { case (id, (_, value)) =>
        assert(test.pageRankSolutions(id) == value)
      }
    }
  }
}
