package com.instagraph.`export`

import org.apache.spark.graphx.Graph
import java.io.PrintWriter

object GraphExporter {
  implicit class GraphExporter[VD, ED](g: Graph[VD, ED]) {
    def toGexf(path: String, fileName: String) = {
      val gexf = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<gexf xmlns=\"http://www.gexf.net/1.3\" version=\"1.3\">\n" +
        "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        "    <nodes>\n" +
        g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
          v._2 + "\" />\n").collect.mkString +
        "    </nodes>\n" +
        "    <edges>\n" +
        g.edges.map(e => "      <edge source=\"" + e.srcId +
          "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
          "\" />\n").collect.mkString +
        "    </edges>\n" +
        "  </graph>\n" +
        "</gexf>"

      val pw = new PrintWriter(path + fileName + ".gexf")
      pw.write(gexf)
      pw.close()
    }
  }
}
