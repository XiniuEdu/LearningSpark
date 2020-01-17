package com.oreilly.learningsparkexamples.mini.scala.chapter11

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object chapter11pipelineapi {

  case class LabeledDocument(id: Long, text: String, label: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)


    val spam = sc.textFile("input\\chapter11\\spam.txt").map(features => LabeledDocument(0, features, 1))
    val normal = sc.textFile("input\\chapter11\\normal.txt").map(features => LabeledDocument(0, features, 0))


    val trainingData = spam ++ normal


    val documents = trainingData // (load RDD of LabeledDocument)
    val documentsDF = sqlContext.createDataFrame(documents)

    // Configure an ML pipeline with three stages: tokenizer, tf, and lr; each stage
    // outputs a column in a SchemaRDD and feeds it to the next stage's input column
    val tokenizer = new Tokenizer() // Splits each email into words
      .setInputCol("text")
      .setOutputCol("words")
    // Maps email words to vectors of 10000 features
    val tf = new HashingTF()
      .setNumFeatures(10000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")


    val lr = new LogisticRegression() // Uses "features" as inputCol by default
    val pipeline = new Pipeline().setStages(Array(tokenizer, tf, lr))
    // Fit the pipeline to the training documents
    val model = pipeline.fit(documentsDF)
    println(model)

    /**
      * 以下代码需要运行于Spark2.x环境，当前环境为1.6.x存在类异常情况
      */
    // Alternatively, instead of fitting once with the parameters above, we can do a
    // grid search over some parameters and pick the best model via cross-validation
    val paramMaps = new ParamGridBuilder()
      .addGrid(tf.numFeatures, Array(10000, 20000))
      .addGrid(lr.maxIter, Array(100, 200))
      .build() // Builds all combinations of parameters
    val eval = new BinaryClassificationEvaluator()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(eval)
    val bestModel = cv.fit(documentsDF)
    bestModel.avgMetrics.foreach(println)
    sc.stop()
  }
}
