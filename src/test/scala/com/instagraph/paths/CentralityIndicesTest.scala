package com.instagraph.paths

import com.instagraph.indices.Indices.Centrality
import com.instagraph.testcases.{TestCase, TestCase1, TestCase2}
import org.scalatest.flatspec.AnyFlatSpec

class CentralityIndicesTest extends AnyFlatSpec {
  val testCases: Seq[TestCase[Int]] = Seq(TestCase1, TestCase2)

  // TODO: Fix Task not serializable
  "Betweenness Centrality computed" should "be approximately the same of the real one" in {
    testCases.foreach { test =>
      test.graph.betweennessCentrality.vertices.foreach { case (id, (_, value)) =>
        assert(value similarTo test.betweennessCentralitySolutions(id.toLong))
      }
    }
  }

  "Degree Centrality computed" should "be approximately the same of the real one" in {
    testCases.foreach { test =>
      test.graph.degreeCentrality.vertices.foreach { case (id, (_, degree)) =>
        assert(degree == test.degreeCentralitySolutions(id.toLong))
      }
    }
  }

  // TODO: Fix Task not serializable
  "Harmonic Centrality computed" should "be approximately the same of the real one" in {
    testCases.foreach { test =>
      test.graph.harmonicCloseness.vertices.foreach { case (id, (_, value)) =>
        assert(value similarTo test.harmonicCentralitySolutions(id.toLong))
      }
    }
  }

  implicit class DoubleUtils(num: Double) {
    def similarTo(other: Double, tol: Double = 1e-3): Boolean = num - other < tol
  }
}
