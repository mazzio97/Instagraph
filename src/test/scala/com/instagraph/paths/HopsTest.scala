package com.instagraph.paths

import ShortestPaths._
import com.instagraph.InstagraphTest
import com.instagraph.testcases.TestCase
import org.apache.spark.graphx.VertexId

class HopsTest extends InstagraphTest {
  runTests { (n: Int, test: TestCase[Int]) =>
    "Shortest paths and fewest hops" should s"coincide for unitary edges graphs from test case $n" in {
      val numVertices: Int = test.graph.numVertices.toInt

      val hops: List[(VertexId, Map[VertexId, Int])] = test.graph
        .fewestHops(Seq.tabulate(numVertices)(i => i.toLong))
        .vertices
        .toLocalIterator
        .toList

      val sp: List[(VertexId, Map[VertexId, Int])] = test.graph
        .mapEdges(_ => 1)
        .allPairsShortestPathsFromOrigin
        .mapVertices((_, spMap) => spMap
          .mapValues(info => info.totalCost)
          .map(identity) // https://github.com/scala/bug/issues/7005
        ).vertices
        .toLocalIterator
        .toList

      assert(hops == sp)
    }
  }
}
