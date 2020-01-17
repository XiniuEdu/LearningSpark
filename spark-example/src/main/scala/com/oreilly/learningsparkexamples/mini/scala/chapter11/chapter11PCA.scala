package com.oreilly.learningsparkexamples.mini.scala.chapter11

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object chapter11PCA {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sparseVec1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6)).map(item => Vectors.dense(item, 2))
    val mat = new RowMatrix(sparseVec1)
    val pc: Matrix = mat.computePrincipalComponents(2)
    val projected = mat.multiply(pc).rows
    val model = KMeans.train(projected, 2, 100)
    /**
      * 以下注释为输出Kmean中心点坐标结果
      */
    //    model.clusterCenters.foreach(item => {
    //      println(item.toJson)
    //    })


    /**
      * 以下方法为SVD 案例解析
      */
    val svd: SingularValueDecomposition[RowMatrix, Matrix] =
      mat.computeSVD(2, computeU = true)
    val U = svd.U // U is a distributed RowMatrix.
    val s = svd.s // Singular values are a local dense vector.
    val V = svd.V
    println("U:"+U.toString)
    println("s:"+s.toString)
    println("V:"+V.toString)

    sc.stop()
  }
}
