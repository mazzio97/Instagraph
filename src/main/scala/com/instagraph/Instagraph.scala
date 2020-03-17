package com.instagraph

import java.nio.file.{Files, Paths}

import com.instagraph.indices.BetweennessCentrality.BetweennessCentrality
import com.instagraph.indices.PageRank.PageRank
import com.instagraph.utils.GraphManager
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

object Instagraph {
  private val resourcesPath: String = "src/main/resources/"
  private val graphPath = resourcesPath + "graph"

  private val sparkConf  = new SparkConf()
    .setMaster("local[*]")  // Master is running on a local node.
    .setAppName("Instagraph") // Name of our spark app

  private val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val manager = new GraphManager[Int](spark)
    // If the graph has already been stored it is loaded, otherwise it is built using the file "data.json"
    val graph: Graph[String, Int] = if (Files.exists(Paths.get(graphPath))) {
      manager.load(graphPath)
    } else {
      val g = manager.build(resourcesPath + "data.json", 1)
      manager.save(g, graphPath)
      g
    }
    // Compute PageRank index on the graph
    println("PageRank:")
    graph.pageRank(0.001)
      .vertices
      .values
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)
    // Compute Betweenness Centrality index on the graph
    // graph.betweennessCentrality.vertices.takeOrdered(10).foreach(println)
  }
}
