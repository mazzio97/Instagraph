package com.instagraph

import com.instagraph.utils.SparkConfiguration._
import org.apache.spark.graphx.Graph

object Main {
  def main(args: Array[String]): Unit = {
    val graph: Graph[String, Int] = Instagraph(sparkConfiguration).unweightedFollowersGraph(
      jsonFiles = "data/giuluck97.json", "data/mazzio97.json"
    )

    println("Vertices: " + graph.vertices.count())
    println("Edges: " + graph.edges.count())
  }
}
