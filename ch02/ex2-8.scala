// Evn: Standalone Applications
// Name: Initializing Spark in Python

// 参考例2-2，统计文件行数的Spark独立应用。
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val conf = new SparkConf().setMaster("local").setAppName("My App")
val sc = new SparkContext(conf)

val lines = sc.textFile("../data/README.md")
val words = line.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>x+y)

println("line count: ",lines.count())
