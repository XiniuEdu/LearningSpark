package com.oreilly.learningsparkexamples.mini.scala

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object chapter4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // 过滤结果长度宽于
    val lines = sc.parallelize(List("holden likes coffee", "panda likes long string and coffee"))
    val pairs = lines.map(x => (x.split(" ")(0), x))
    val outPairs = pairs.filter({ case (key, value) => value.length < 20 })
    // 过滤结果输出
    //outPairs.collect().foreach(println)

    val rdd = sc.parallelize(List(("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    val rddReduce = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    // reduceByKey结果输出
    //rddReduce.collect().foreach(println)

    val input = sc.textFile("input\\README.md")
    val words = input.flatMap(x => x.split(" "))
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    //result.take(10).foreach(println)

    val result2 = result.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }
    println("CombineByKey结果输出:")
    //    result2.take(10).foreach(println(_))

    val data = Seq(("a", 3), ("b", 4), ("a", 1))
    // 使用默认分区数进行运算
    val paraDefault = sc.parallelize(data).reduceByKey((x, y) => x + y)
    println("默认分区方法分区数：" + paraDefault.getNumPartitions)
    val paraCustom = sc.parallelize(data).reduceByKey((x, y) => x + y, 10)
    println("设置分区数后的分区数：" + paraCustom.getNumPartitions)


    /**
      * Join方法案例
      */
    case class Store(name: String)
    val storeAddress = sc.parallelize(List(
      (Store("Ritual"), "1026 Valencia St")
      , (Store("Philz"), "748 Van Ness Ave")
      , (Store("Philz"), "3101 24th St")
      , (Store("Starbucks"), "Seattle")
    ))
    val storeRating = sc.parallelize(List(
      (Store("Ritual"), 4.9),
      (Store("Philz"), 4.8)
    ))
    storeAddress.join(storeRating).collect.foreach(println)

    /**
      * sortByKey
      */


    val sortData = sc.parallelize(List(
      (111, "111"),
      (19, "19")
    ))
    val sortRdd = sortData.sortByKey()
    println("普通排序结果：")
    sortRdd.collect.foreach(println)
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
    }
    val sortRdd2 = sortRdd.sortByKey()
    println("重载排序算法后的排序结果：")
    sortRdd2.collect.foreach(println)


    /**
      * PageRank
      */
    val links = sc.textFile("input\\links.txt").map(items => {
      val words = items.split(" ")
      (words(0).toString, words(1).toString)
    })


    var ranks = links.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (pageLinks, rank)) =>
          pageLinks.map(dest => (dest.toString, rank / pageLinks.size))
      }

      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }
    println("输出PageRank算法结果：")
    ranks.collect().foreach(println)


    /**
      * 自定义 Partitioner
      */
    class DomainNamePartitioner(numParts: Int) extends Partitioner {
      override def getPartition(key: Any): Int = {
        val domain = new java.net.URL(key.toString).getHost()
        val code = (domain.hashCode % numPartitions)
        if (code < 0) {
          code + numPartitions
        }
        else {
          code
        }
      }

      override def equals(other: scala.Any): Boolean = other match {
        case dnp: DomainNamePartitioner =>
          dnp.numPartitions == numPartitions
        case _ =>
          false
      }

      override def numPartitions: Int = numParts
    }

  }
}

