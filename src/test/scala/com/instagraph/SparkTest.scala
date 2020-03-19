package com.instagraph

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

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
}
