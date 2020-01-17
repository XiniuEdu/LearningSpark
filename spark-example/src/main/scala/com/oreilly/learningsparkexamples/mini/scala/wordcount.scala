package com.oreilly.learningsparkexamples.mini.scala

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val inputFile = "input\\README.md"
    val outputFile ="output"
    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // 未配置Hadoop环境的机器请使用控制台日志输出，saveAsTextFile需要hadoop环境
    //counts.saveAsObjectFile(outputFile)
    counts.collect().foreach(println)
  }
}
