package com.instagraph

import com.instagraph.indices.Indices.Centrality
import com.instagraph.indices.centrality.{BetweennessCentrality, DegreeCentrality, HarmonicCloseness, PageRank}
import com.instagraph.utils.GraphUtils.Manipulations
import com.instagraph.utils.GraphUtils.Stats
import com.instagraph.utils.GraphUtils.withElapsedTime
import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]): Unit = {
    val graph = Instagraph(new SparkConf().setMaster("local[*]").setAppName("Instagraph")).unweightedFollowersGraph(
      "data/giuluck97.json", "data/mazzio97.json" 
    ) without("mazzio97", "giuluck97")
    graph.cache()

    println("Vertices: " + graph.numVertices)
    println("Edges: " + graph.numEdges)

    Map(
      "Betweenness Centrality" -> BetweennessCentrality[Int](),
      "Harmonic Centrality" -> HarmonicCloseness[Int](),
      "PageRank" -> PageRank[Int]()
    ).foreach { case (name, index) =>
      print(s"\nComputing $name... ")
      val ranking = withElapsedTime(graph transformWith index) topVertices()
      ranking.zipWithIndex.map(_.swap).foreach { case (pos, (user, value)) =>
        println(s"${pos+1}) $user -> $value")
      }
    }
  }
}
