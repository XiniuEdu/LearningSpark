package com.oreilly.learningsparkexamples.mini.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object chapter3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //基于数组创建
    val lines1 = sc.parallelize(List("pandas", "i like pandas"))
    //基于文件创建
    val lines2 = sc.textFile("input\\README.md")
    println("基于数组创建的数据集的第一行：" + lines1.first())
    println("基于文件创建的数据集的第一行：" + lines2.first())


    val inputRDD = sc.textFile("input\\log.txt")
    // 过滤日志中存在error字样的行
    val errorsRDD = inputRDD.filter(line => {
      line.contains("error")
    })
    // 过滤日志中存在warn字样的行
    val warningsRDD = inputRDD.filter(line => {
      line.contains("warn")
    })
    // 将异常记录结果进行连接
    val badLinesRDD = errorsRDD.union(warningsRDD)
    // 将前十个结果收集到控制台并输出
    println("Input had " + badLinesRDD.count() + " concerning lines")
    println("Here are 10 examples:")
    //badLinesRDD.take(10).foreach(println)
    /**
      * Lazy特性测试-函数式方法
      */
    class SearchFunctions(val query: String) extends Serializable {
      def getMatchesFunctionReference(rdd: RDD[String]): RDD[Boolean] = {
        // Problem: "isMatch" means "this.isMatch", so we pass all of "this"
        rdd.map(isMatch)
      }

      def isMatch(s: String): Boolean = {
        s.contains(query)
      }

      def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
        // Problem: "query" means "this.query", so we pass all of "this"
        rdd.map(x => x.split(query))
      }

      def getMatchesNoReference(rdd: RDD[String]): RDD[Array[String]] = {
        // Safe: extract just the field we need into a local variable
        val query_ = this.query
        rdd.map(x => x.split(query_))
      }
    }
    val query = " "
    val c1 = new SearchFunctions(query)
    val linesLazy = sc.parallelize(List("hello world", "hi"))
    val resultLazy1 = c1.getMatchesFunctionReference(linesLazy)
    val resultLazy2 = c1.getMatchesFieldReference(linesLazy)
    val resultLazy3 = c1.getMatchesNoReference(linesLazy)

    resultLazy1.collect().foreach(println)
    resultLazy2.collect().foreach(_.foreach(println))
    resultLazy3.collect().foreach(_.foreach(println))

    // map方法-Double execution-计算案例
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println("Map 方法结果" + result.collect().mkString(","))

    // flatMap方法案例
    val lines = sc.parallelize(List("hello world", "hi"))
    val words = lines.flatMap(line => line.split(" "))
    println("FlatMap 结果：" + words.first())

    //reduce 方法计算案例
    val inputReduce = sc.parallelize(List(1, 2, 3, 4))
    val sum = inputReduce.reduce((x, y) => x + y)
    println("Reduce 结果：" + sum)
    // aggregate 方法计算案例
    val resultAgg = inputReduce.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = resultAgg._1 / resultAgg._2.toDouble
    println("Agg 结果" + avg)

    //Persist 缓存方法案例
    val inputPersist = sc.parallelize(List(1, 2, 3, 4))
    val resultPersist = inputPersist.map(x => x * x)
    //对于resultPersist对象进行缓存，只缓存在磁盘上
    resultPersist.persist(StorageLevel.DISK_ONLY)
    println(resultPersist.collect().mkString(","))


  }
}
