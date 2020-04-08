package com.instagraph

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag

case class Instagraph(sparkConf: SparkConf = new SparkConf().setAppName("Instagraph")) {
  private val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  private val sparkContext: SparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  def followersGraph[E: ClassTag](edgeWeight: Set[String] => E, jsonFiles: String*): Graph[String, E] = {
    // RDD containing the future nodes of the graph (both id and value, namely the username)
    // associated to their respective list of following accounts
    val usersRDD: RDD[(Long, String, Set[String])] = sparkContext.parallelize(
      jsonFiles.map(spark.read.json(_))
        .flatMap(df => df.head.getValuesMap[mutable.WrappedArray[String]](df.columns))
        .groupBy { case (username, _) => username }
        .mapValues(_.flatMap { case (_, following) => following }.toSet)
        .toSeq
    ).zipWithIndex().map { case ((username, following), id) => (id, username, following) }

    // a local map of the users indexed by value useful for a faster computation of the relationships (the edges)
    val usersMap: Map[String, Long] = usersRDD.map { case (id, username, _) => (username, id) }.collectAsMap().toMap

    // the RDD of edges is created from the RDD of users and their respective following
    val edges: RDD[Edge[E]] = usersRDD.flatMap { case (id, _, following) =>
      val weight: E = edgeWeight(following)
      following.map(usersMap.get)
        .filter(followingOption => followingOption.isDefined)
        .map(followingId => Edge(id, followingId.get, weight))
    }

    Graph(usersRDD.map { case(id, username, _) => (id, username) }, edges)
  }

  def unweightedFollowersGraph(jsonFiles: String*): Graph[String, Int] = followersGraph[Int](_ => 1, jsonFiles:_*)
}