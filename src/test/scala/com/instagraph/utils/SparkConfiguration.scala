package com.instagraph.utils

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object SparkConfiguration {
  val sparkConfiguration: SparkConf = new SparkConf()
    .setMaster("local[*]")  // Master is running on a local node.
    .setAppName("InstagraphTest") // Name of our spark app

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConfiguration)
    .getOrCreate()

  val sparkContext: SparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("ERROR")

  def createGraph[E: ClassTag](edges: RDD[Edge[E]]): Graph[Int, E] = {
    val numVertices: Int = edges.map(triplet => math.max(triplet.srcId, triplet.dstId)).max().toInt + 1
    val vertices: RDD[(Long, Int)] = sparkContext.makeRDD(Seq.tabulate(numVertices)(i => (i.toLong, i)))
    Graph(vertices, edges)
  }
}
