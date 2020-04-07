package com.instagraph.utils

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class GraphManager[E: ClassTag](spark: SparkSession) {
  private val sparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  def build(edgeValue: (String, String) => E, jsonPaths: String*): Graph[String, E] = {
    val df = jsonPaths.map(spark.read.json(_))
    val users = df.flatMap(_.columns).distinct
    // Vertices of the graph are the users identified with an ID
    val vertices: RDD[(VertexId, String)] = sparkContext.parallelize(
      users.zipWithIndex.map { case (user, idx) => (idx.toLong, user) }
    )
    // Extract the ID of a vertex given the username
    val correspondence: String => VertexId = user => vertices.filter(v => v._2 == user).first()._1
    // Edges of the graph are following relations between any pair of vertices
    val edges: RDD[Edge[E]] = sparkContext.parallelize(df.flatMap(
      _.collect()(0) // Extract first (and only) row for each column (user)
        .getValuesMap[Seq[String]](users) // Map each vertex to its following
        .mapValues[Seq[String]](_.filter(users.contains)) // Don't consider following outward vertices
        .flatten { case (user, foll) => foll.map((user, _)) } // Convert map into pairs representing edges
        .map(e => Edge(correspondence(e._1), correspondence(e._2), edgeValue)) // GraphX representation
        .toSeq
    ))
    Graph(vertices, edges)
  }

  def load(path: String): Graph[String, E] = {
    val vertices = sparkContext.objectFile[(VertexId, String)](path + "/vertices")
    val edges = sparkContext.objectFile[Edge[E]](path + "/edges")
    Graph(vertices, edges)
  }

  def save(graph: Graph[String, E], path: String): Unit = {
    graph.vertices.saveAsObjectFile(path + "/vertices")
    graph.edges.saveAsObjectFile(path + "/edges")
  }
}
