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
  }
}
