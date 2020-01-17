package com.oreilly.learningsparkexamples.mini.scala.chapter8

import org.apache.spark.{SparkConf, SparkContext}

object chapter8base {
  def main(args: Array[String]): Unit = {
    // Construct a conf
    val conf = new SparkConf()
    conf.set("spark.app.name", "My Spark App")
    conf.set("spark.master", "local[4]")
    conf.set("spark.ui.port", "36000") // Override the default port
    // Create a SparkContext with this configuration
    val sc = new SparkContext(conf)

    val input = sc.textFile("input/links.txt")
    // Split into words and remove empty lines
    val tokenized = input.
      filter(line => line.length > 0).
      map(line => line.split(" "))
    // Extract the first word from each line (the log level) and do a count
    val counts = tokenized.
      map(words => (words(0), 1)).
      reduceByKey { (a, b) => a + b }
    println(input.toDebugString)
    println(counts.toDebugString)

  }
}
