package com.oreilly.learningsparkexamples.mini.scala.chapter9

// Import Spark SQL

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
// Or if you can't have the hive dependencies
import org.apache.spark.sql.SQLContext

object chapter9base {

  case class HappyPerson(handle: String, favouriteBeverage: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //若无Hive环境，则使用 SQLContext
    val hiveCtx = new SQLContext(sc)

    val input = hiveCtx.jsonFile("input/chapter9-sparksql-json.txt")
    // 将数据注册成临时表便于sql操作
    input.registerTempTable("tweets")
    val topTweets = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    topTweets.show()
    topTweets.printSchema()
    /**
      * 将DataFrame 转换成RDD进行操作
      */
    val topTweetText = topTweets.rdd.map(row => row.getString(0))
    topTweetText.collect.foreach(println)

    val input2 = hiveCtx.jsonFile("input/chapter9-sparksql-json2.txt")
    input2.printSchema()

    /**
      * 从RDD创建DataFrame
      */
    val happyPeopleRDD = sc.parallelize(Seq(HappyPerson("holden", "coffee")))

    //将case class的RDD通过hiveCtx的createDataFrame转换成DataFrame用于进行SparkSQL操作
    val happyPeopleDF = hiveCtx.createDataFrame(happyPeopleRDD)
    happyPeopleDF.registerTempTable("happy_people")

    /**
      * 创建UDF函数
      */
    hiveCtx.udf.register("strLenScala", (_: String) .length)
    //注意：书中strLenScala('tweet')为计算字符串'tweet'的长度，而非tweet列值的长度。
    //为了便于理解，此处加上了tweet列的原始值及调用udf函数后的结果
    val tweetLength = hiveCtx.sql("SELECT tweet,strLenScala(tweet) FROM tweets LIMIT 10")
    tweetLength.show

  }
}
