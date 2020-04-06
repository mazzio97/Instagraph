package com.instagraph

import com.instagraph.testcases._
import org.scalatest.flatspec.AnyFlatSpec

trait InstagraphTest extends AnyFlatSpec {
  def runTests(tests: (Int, TestCase[Int]) => _): Unit = {
    Map(1 -> TestCase1, 2 -> TestCase2).foreach { case (n, test) => tests(n, test) }
  }
}
