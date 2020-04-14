package com.instagraph.utils

import java.io.PrintWriter

import com.instagraph.indices.Indices.Centrality
import com.instagraph.indices.centrality.CentralityIndex
import org.apache.spark.graphx.{Edge, Graph}

import scala.reflect.ClassTag

object GraphUtils {
  implicit class Manipulations[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def without(vertices: V*): Graph[V, E] = graph.subgraph(e =>
      !vertices.contains(e.srcAttr) && !vertices.contains(e.dstAttr),
      (_, user) => !vertices.contains(user)
    )

    def mergeWith(otherGraph: Graph[V, E]): Graph[V, E] = {
      val vertices = graph.vertices.map(_._2).union(otherGraph.vertices.map(_._2)).distinct.zipWithIndex
      def updateEdges(g: Graph[V, E]) =
        g.triplets
          .map(et => (et.srcAttr, (et.attr, et.dstAttr)))
          .join(vertices)
          .map { case (oldIdSrc, ((edge, dstId), newIdSrc)) => (dstId, (newIdSrc, edge)) }
          .join(vertices)
          .map { case (oldIdDst, ((newIdSrc, edge), newIdDst)) => new Edge(newIdSrc, newIdDst, edge) }
      Graph(
        vertices.map(_.swap),
        updateEdges(graph) union updateEdges(otherGraph)
      )
    }

    def onlyBidirectional: Graph[V, E] = {
      graph.subgraph(epred = e1 =>
        graph.edges.filter(e2 => e1 == Edge(e2.dstId, e2.srcId, e2.attr)).count() > 0
      )
    }
  }

  implicit class Export[V, E](graph: Graph[V, E]) {
    def exportToGEXF(filePath: String): Unit = {
      val gexf = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2draft\">\n" +
        "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        "    <nodes>\n" +
        graph.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
          v._2 + "\" />\n").collect.mkString +
        "    </nodes>\n" +
        "    <edges>\n" +
        graph.edges.map(e => "      <edge source=\"" + e.srcId +
          "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
          "\" />\n").collect.mkString +
        "    </edges>\n" +
        "  </graph>\n" +
        "</gexf>"

      val pw = new PrintWriter(filePath)
      pw.write(gexf)
      pw.close()
    }
  }

  implicit class Stats[V: ClassTag, M: ClassTag, E: ClassTag](graph: Graph[(V, M), E]) {
    def topVertices(n: Int = 10)(implicit ord: Ordering[M]): Iterable[(V, M)] =
      graph.vertices
        .map(_._2)
        .sortBy(_._2, ascending = false)
        .take(n)
  }

  def withElapsedTime[V: ClassTag, M: ClassTag, E: ClassTag](operation: => Graph[(V, M), E]): Graph[(V, M), E] = {
    val begin = System.currentTimeMillis()
    val result = operation
    val end = System.currentTimeMillis()
    println("Elapsed time: " + (end - begin).toDouble / 1000 + " seconds")
    result
  }
}
