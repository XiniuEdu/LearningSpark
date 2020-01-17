package com.oreilly.learningsparkexamples.mini.scala.chapter11

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object chapter11Vectors {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val denseVec1 = Vectors.dense(1.0, 2.0, 3.0)
    val denseVec2 = Vectors.dense(Array(1.0, 2.0, 3.0))
    val sparseVec1 = Vectors.sparse(4,Array(0,2),Array(1.0,2.0))
    sc.stop()
  }
}
