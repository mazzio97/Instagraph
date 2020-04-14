package com.instagraph

import com.instagraph.indices.centrality.{BetweennessCentrality, HarmonicCloseness, PageRank}
import com.instagraph.indices.Indices.Centrality
import com.instagraph.utils.GraphUtils.Manipulations
import com.instagraph.utils.GraphUtils.Stats
import com.instagraph.utils.GraphUtils.withElapsedTime
import com.instagraph.utils.SparkConfiguration._
import org.apache.spark.graphx.Graph

object Main {
  def main(args: Array[String]): Unit = {
    val graph: Graph[String, Int] = Instagraph(sparkConfiguration).unweightedFollowersGraph(
      jsonFiles = "data/giuluck97.json", "data/mazzio97.json"
    ) without("mazzio97", "giuluck97")
    graph.cache()

    println("Vertices: " + graph.vertices.count())
    println("Edges: " + graph.edges.count())

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
