package com.instagraph

import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]): Unit = {
    val graph = Instagraph(new SparkConf().setMaster("local[*]").setAppName("Instagraph")).unweightedFollowersGraph(
      "data/giuluck97.json", "data/mazzio97.json" 
    )
    println("vertices: " + graph.numVertices)
    println("edges: " + graph.numEdges)
  }
}
