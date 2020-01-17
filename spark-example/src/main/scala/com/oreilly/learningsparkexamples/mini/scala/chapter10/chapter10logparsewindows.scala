package com.oreilly.learningsparkexamples.mini.scala.chapter10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object chapter10logparsewindows {
  def main(args: Array[String]): Unit = {
    /**
      * 注意Streaming应用由于涉及到需要线程监控数据，说有本机执行时需要至少保证2个线程，也就是local[2]
      */
    val conf = new SparkConf().setAppName("sparkStreaming").setMaster("local[5]")
    //为了便于查看结果，推荐学习时候将Seconds修改为5秒处理一次
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("output\\checkpoint")

    // windows可以使用nc -L -p 7777命令进行端口信息发送
    /**
      * 输入日志内容如下：
      *1.1.1.1 - - [21/Jul/2014:10:00:00 -0800] "GET /majihua/article/284234 HTTP/1.1" 200 1234
      *1.1.1.1 - - [21/Jul/2014:10:00:00 -0800] "GET /majihua/article/284234 HTTP/1.1" 200 2000
      *1.1.1.1 - - [21/Jul/2014:10:00:00 -0800] "GET /majihua/article/284234 HTTP/1.1" 401 10
      *192.13.212.25 - - [04/Aug/2014:15:18:27 +0800] "GET /abc/ HTTP/1.1" 200 280
      *
      */
    val logData = ssc.socketTextStream("localhost", 7777).filter(_.length > 0)
    /**
      * 推荐加入两部过滤，确保数据处理过程中不会出现异常情况
      */
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseLogLine(line)).filter(null != _.ipAddress)
    val ipDStream = accessLogDStream.map(entry => (entry.ipAddress, 1))
    val ipCountsDStreamWindows = ipDStream.reduceByKeyAndWindow(
      { (x, y) => x + y },
      { (x, y) => x - y },
      Seconds(30),
      Seconds(10)
    )
    ipCountsDStreamWindows.print(2)


    ssc.start()
    ssc.awaitTermination()
  }
}
