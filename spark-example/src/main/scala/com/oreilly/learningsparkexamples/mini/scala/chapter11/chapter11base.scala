package com.oreilly.learningsparkexamples.mini.scala.chapter11

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object chapter11base {


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
    val lrLearner = new LogisticRegressionWithSGD()
    val model = lrLearner.run(trainingData)
    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")

    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")
    sc.stop()
  }
}
