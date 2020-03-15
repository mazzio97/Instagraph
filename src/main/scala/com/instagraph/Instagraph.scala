package com.instagraph

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Instagraph {

  private val sparkConf  = new SparkConf()
    .setMaster("local[*]")  // Master is running on a local node.
    .setAppName("Instagraph") // Name of our spark app

  private val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val df = spark.read.json("src/main/resources/data.json")
    df.printSchema()
    // df.show()

    val users = df.columns

    val vertices: RDD[(VertexId, String)] = spark.sparkContext.parallelize(
      users.zipWithIndex.map { case (user, idx) => (idx.toLong, user) }
    )

    // Extract the ID of a vertex given the username
    val correspondence: String => VertexId = user => vertices.filter(v => v._2 == user).first()._1

    val edges: RDD[Edge[String]] = spark.sparkContext.parallelize(
      df.collect()(0) // Extract first (and only) row for each column (user)
        .getValuesMap[Seq[String]](users) // Map each vertex to its following
        .mapValues[Seq[String]](_.filter(users.contains)) // Don't consider following outward vertices
        .flatten { case (user, foll) => foll.map((user, _)) } // Convert map into pairs representing edges
        .map( e => Edge(correspondence(e._1), correspondence(e._2), "follows") ) // GraphX representation
        .toArray
    )

    val graph = Graph(vertices, edges)

    graph.triplets.takeSample(withReplacement = false, num = 10).foreach(println)
  }
}
