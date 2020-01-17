package com.oreilly.learningsparkexamples.mini.scala.chapter5

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.KeyValueTextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object chapter5Base {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /**
      * load TextFile
      */
    val inputFile = "input\\chapter5.txt"
    val input = sc.wholeTextFiles(inputFile)
    val inputCompute = input.mapValues(y => {
      val nums = y.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    })
    //    inputCompute.collect().foreach(println)

    /**
      * load Json File
      */
    val inputJson = sc.textFile("input\\chapter5-json.txt")
    val result = inputJson.flatMap(record => {
      try {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        Some(mapper.readValue(record, classOf[PersonJson]))
      }
      catch {
        case e: Exception => {
          None
        }
      }
    })
    //    result.collect().foreach(println)

    /**
      * load csv file - textFile
      * @TODO wholeTextFile's code 有异常
      */
    val inputCsv = sc.textFile("input\\chapter5-csv.txt")
    val resultCsv = inputCsv.map(line => {
      val reader = new CSVReader(new StringReader(line));
      val x = reader.readNext()
      PersonCsv(x(0), x(1))
    })
    //resultCsv.collect().foreach(println)

    /**
      * Save SequenceFile files
      */
    val inputSequence = sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2)))
    inputSequence.saveAsSequenceFile("output/c5")

    /**
      * Load From HadoopFile
      */

    val inputHdfsFile = sc.hadoopFile[Text, Text, KeyValueTextInputFormat](inputFile).map{
      case (x, y) => (x.toString, y.toString) }

    /**
      *@TODO Load from LzoJsonfile and some LzoDataExample
      *      val inputLzoJson = sc.newAPIHadoopFile(inputFile, classOf[LzoJsonInputFormat],
      *      classOf[LongWritable], classOf[MapWritable], conf)
      */



    sc.stop()

  }

  case class PersonJson(name: String, lovesPandas: Boolean)

  case class PersonCsv(name: String, favoriteAnimal: String)


}
