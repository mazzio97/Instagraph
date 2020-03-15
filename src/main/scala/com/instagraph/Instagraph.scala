package com.instagraph

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Instagraph {

  private val resourcesPath = "src/main/resources/"

  private val sparkConf  = new SparkConf()
    .setMaster("local[*]")  // Master is running on a local node.
    .setAppName("Instagraph") // Name of our spark app

  private val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  private def sparkContext = spark.sparkContext

  sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    def buildGraph(path: String): Graph[String, String] = {

      val df = spark.read.json(path)
      // df.printSchema()
      // df.show()

      val users = df.columns

      val vertices: RDD[(VertexId, String)] = sparkContext.parallelize(
        users.zipWithIndex.map { case (user, idx) => (idx.toLong, user) }
      )

      // Extract the ID of a vertex given the username
      val correspondence: String => VertexId = user => vertices.filter(v => v._2 == user).first()._1

      val edges: RDD[Edge[String]] = sparkContext.parallelize(
        df.collect()(0) // Extract first (and only) row for each column (user)
          .getValuesMap[Seq[String]](users) // Map each vertex to its following
          .mapValues[Seq[String]](_.filter(users.contains)) // Don't consider following outward vertices
          .flatten { case (user, foll) => foll.map((user, _)) } // Convert map into pairs representing edges
          .map( e => Edge(correspondence(e._1), correspondence(e._2), "follows") ) // GraphX representation
          .toSeq
      )

      Graph(vertices, edges)
    }

    // if the graph has already been stored it is loaded, otherwise it is built using the file "data.json"
    val graph: Graph[String, String] = if (Files.exists(Paths.get(resourcesPath + "graph"))) {
      val vertices = sparkContext.objectFile[(VertexId, String)](resourcesPath + "graph/vertices")
      val edges = sparkContext.objectFile[Edge[String]](resourcesPath + "graph/edges")
      Graph(vertices, edges)
    } else {
      val graph = buildGraph(resourcesPath + "data.json")
      graph.vertices.saveAsObjectFile(resourcesPath + "graph/vertices")
      graph.edges.saveAsObjectFile(resourcesPath + "graph/edges")
      graph
    }

    println("Vertices: " + graph.vertices.count())
    println("Edges: " + graph.edges.count())
    println("Samples:")
    graph.triplets.takeSample(withReplacement = false, num = 10).foreach(println)
  }
}