package com.instagraph

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Instagraph {
  def main(args: Array[String]): Unit = {
    val sparkConf  = new SparkConf()
      .setMaster("local[*]")  // Master is running on a local node.
      .setAppName("Instagraph") // Name of our spark app

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df = spark.read.json("src/main/resources/data.json")
    df.printSchema()
    df.show()

    val vertices = df.columns.toSeq
    val edges: Map[String, Seq[String]] = df.collect()(0) // Extract first (and only) row for each column (user)
      .getValuesMap[Seq[String]](vertices) // Map each vertex to its following
      .mapValues[Seq[String]](_.filter(vertices.contains)) // Don't consider following outward vertices

    edges.foreach { case (user: String, foll: Seq[String]) =>
      println(user + " -> " + foll)
    }
  }
}
