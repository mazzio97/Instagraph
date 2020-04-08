package com.instagraph

import java.nio.file.{Files, Paths}

import com.instagraph.utils.GraphManager
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object Instagraph {
  private val graphsPath = System.getProperty("user.home") + "/.instagraph/"

  private val sparkConf = new SparkConf().setAppName("Instagraph")

  private val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  def followersGraph[E: ClassTag](edgeWeight: Seq[String] => E, jsonFiles: String*): Graph[String, E] = {
    val manager = new GraphManager[E](spark)
    val graphName = jsonFiles.map(path => path.split("/").last)
      .map(name => name.takeWhile(c => c != '.'))
      .sorted
      .mkString("+")
    val graphPath = graphsPath + graphName
    // If the graph has already been stored it is loaded, otherwise it is built and saved for future loadings
    if (Files.exists(Paths.get(graphPath))) {
      manager.load(graphPath)
    } else {
      val g = manager.build(edgeWeight, jsonFiles:_*)
      manager.save(g, graphPath)
      g
    }
  }

  def unweightedFollowersGraph(jsonFiles: String*): Graph[String, Int] = followersGraph[Int](_ => 1, jsonFiles:_*)
}