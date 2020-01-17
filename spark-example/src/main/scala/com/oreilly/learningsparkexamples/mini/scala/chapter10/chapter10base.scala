package com.oreilly.learningsparkexamples.mini.scala.chapter10

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object chapter10base {
  def main(args: Array[String]): Unit = {
    /**
      * 注意Streaming应用由于涉及到需要线程监控数据，说有本机执行时需要至少保证2个线程，也就是local[2]
      */
    val conf = new SparkConf().setAppName("sparkStreaming").setMaster("local[2]")
    //为了便于查看结果，推荐学习时候将Seconds修改为10秒处理一次
    val ssc = new StreamingContext(conf, Seconds(10))
    // windows可以使用nc -L -p 7777命令进行端口信息发送
    val lines = ssc.socketTextStream("localhost", 7777)
    // 过滤出所有含有error字样的记录
    val errorLines = lines.filter(_.contains("error"))
    // 输出错误的相关记录数
    errorLines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
