package com.oreilly.learningsparkexamples.mini.scala.chapter5


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Structured Data with Spark SQL
  */


object SparkSqlExample {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val input = sc.textFile("input/chapter5_sparksql.txt").map(line => {
      val words = line.split(" ")
      Person(words(0), words(1).toInt)
    })

    /**
      * SparkSQL 初试
      * 注意：若开发机无法连接Hive环境，推荐使用SQLContext方法，
      * 无Hive环境使用HiveContext会直接报错。
      */
    val hiveCtx = new SQLContext(sc)
    val df = hiveCtx.createDataFrame(input)
    df.registerTempTable("users")
    val rows = hiveCtx.sql("SELECT name, age FROM users")
    //    val firstRow = rows.first()
    //    println(firstRow.getString(0))

    /**
      * SparkSql读取Json格式文件
      */
    val tweets = hiveCtx.jsonFile("input/chapter5-sparksql-json.txt")
    tweets.registerTempTable("tweets")
    val results = hiveCtx.sql("SELECT user.name, text FROM tweets")
    //    results.show()


  }
}
