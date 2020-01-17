package com.oreilly.learningsparkexamples.mini.scala.chapter6

import org.apache.spark.{SparkConf, SparkContext}

object chapter6base {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val inputFile = "input/chapter6_base.txt"

    /**
      * 累加器使用概要
      */
    val file = sc.textFile(inputFile)
    val blankLines = sc.accumulator(0) // Create an Accumulator[Int] initialized to 0
    val callSigns = file.flatMap(line => {
      if (line == "") {
        blankLines += 1 // Add to the accumulator
      }
      line.split(" ")
    })
    // 注意要通过action触发任务执行，不然累加器的值依旧为0
    callSigns.collect()
    //    callSigns.saveAsTextFile("output.txt")
    println("Blank lines: " + blankLines.value)

    /**
      * @TODO Broadcast 广播变量
      */
    //    val signPrefixes = sc.broadcast(loadCallSignTable())
    //    val countryContactCounts = contactCounts.map{case (sign, count) =>
    //      val country = lookupInArray(sign, signPrefixes.value)
    //      (country, count)
    //    }.reduceByKey((x, y) => x + y)
    //    countryContactCounts.saveAsTextFile(outputDir + "/countries.txt")

    /**
      * Number RDD Operations
      */

    val distanceDoubles = callSigns.filter(_.length>0).map(string => string.toDouble)
    val stats = distanceDoubles.stats()
    val stddev = stats.stdev
    val mean = stats.mean
    val reasonableDistances = distanceDoubles.filter(x => math.abs(x-mean) < 3 * stddev)
    println(reasonableDistances.collect().toList)
  }
}
