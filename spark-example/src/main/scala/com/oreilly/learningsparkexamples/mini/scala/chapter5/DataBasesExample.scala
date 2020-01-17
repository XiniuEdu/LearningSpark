package com.oreilly.learningsparkexamples.mini.scala.chapter5

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.io.MapWritable
//import org.apache.hadoop.mapred.JobConf

object DataBasesExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /**
      * SparkSQL读取JDBC数据文件
      * 注意：需要提前准备本机的数据库环境
      */
    def createConnection() = {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      DriverManager.getConnection("jdbc:mysql://localhost/test?user=holden");
    }

    def extractValues(r: ResultSet) = {
      (r.getInt(1), r.getString(2))
    }

    val data = new JdbcRDD(sc,
      createConnection, "SELECT * FROM panda WHERE ? <= id AND id <= ?",
      lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)

    /**
      * @TODO Cassandra 数据读取接口
      */
    //
    //    /**
    //      * Hbase读取接口
    //      */
    //
    //    val conf = HBaseConfiguration.create()
    //    conf.set(TableInputFormat.INPUT_TABLE, "tablename") // which table to scan
    //    val rdd = sc.newAPIHadoopRDD(
    //      conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    //
    //
    //    /**
    //      * ES input
    //      */
    //    def mapWritableToInput(in: MapWritable): Map[String, String] = {
    //      in.map { case (k, v) => (k.toString, v.toString) }.toMap
    //    }
    //
    //    val jobConf = new JobConf(sc.hadoopConfiguration)
    //    jobConf.set(ConfigurationOptions.ES_RESOURCE_READ, args(1))
    //    jobConf.set(ConfigurationOptions.ES_NODES, args(2))
    //    val currentTweets = sc.hadoopRDD(jobConf,
    //      classOf[EsInputFormat[Object, MapWritable]], classOf[Object],
    //      classOf[MapWritable])
    //    // Extract only the map
    //    // Convert the MapWritable[Text, Text] to Map[String, String]
    //    val tweets = currentTweets.map { case (key, value) => mapWritableToInput(value) }
    //
    //    /**
    //      * ES Output
    //      */
    //    val jobConf = new JobConf(sc.hadoopConfiguration)
    //    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    //    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    //    jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, "twitter/tweets")
    //    jobConf.set(ConfigurationOptions.ES_NODES, "localhost")
    //    FileOutputFormat.setOutputPath(jobConf, new Path("-"))
    //    output.saveAsHadoopDataset(jobConf)
  }
}
