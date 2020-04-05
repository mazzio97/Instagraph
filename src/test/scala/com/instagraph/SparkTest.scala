package com.instagraph

import com.instagraph.paths.allpairs.adjacents.EachPathWeightedInfo
import com.instagraph.paths.allpairs.fullroutes.EachFullRouteInfo
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

trait SparkTest {
  private val sparkConf  = new SparkConf()
    .setMaster("local[*]")  // Master is running on a local node.
    .setAppName("InstagraphTest") // Name of our spark app

  private val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sparkContext: SparkContext = spark.sparkContext
  sparkContext.setLogLevel("ERROR")

  def createGraph[E: ClassTag](edges: RDD[Edge[E]]): Graph[Int, E] = {
    val numVertices: Int = edges.map(triplet => math.max(triplet.srcId, triplet.dstId)).max().toInt + 1
    val vertices: RDD[(Long, Int)] = sparkContext.makeRDD(Seq.tabulate(numVertices)(i => (i.toLong, i)))
    Graph(vertices, edges)
  }
}

case class TestCase[E: ClassTag](
  graph: Graph[Int, E],
  adjacentsSolutions: Map[VertexId, Map[VertexId, EachPathWeightedInfo[E]]],
  fullRoutesSolutions: Map[VertexId, EachFullRouteInfo[E]]
)
