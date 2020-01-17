package com.oreilly.learningsparkexamples.mini.scala.chapter11

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object chapter11Linear {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    val spam = sc.textFile("input\\chapter11\\spam.txt")
    val normal = sc.textFile("input\\chapter11\\normal.txt")
    val tf = new HashingTF(10000)
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))

    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))

    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))

    val trainingData = positiveExamples ++ negativeExamples

    trainingData.cache()
    val lr = new LinearRegressionWithSGD().setIntercept(true)
    val model = lr.run(trainingData)
    println("weight: %s,intercep: %s".format(model.weights, model.intercept))
    sc.stop()
  }
}
