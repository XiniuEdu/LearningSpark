package com.oreilly.learningsparkexamples.mini.scala.chapter10

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object chapter10kafka {
  def main(args: Array[String]): Unit = {
    /**
      * 注意Streaming应用由于涉及到需要线程监控数据，说有本机执行时需要至少保证2个线程，也就是local[2]
      */
    val conf = new SparkConf().setAppName("sparkStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    //为了便于查看结果，推荐学习时候将Seconds修改为10秒处理一次
    val brokers = "172.15.4.215:2181"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicSet = List("pandas", "logs").toSet
    val topicLines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    topicLines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
