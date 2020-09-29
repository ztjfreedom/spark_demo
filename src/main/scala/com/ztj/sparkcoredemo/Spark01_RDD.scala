package com.ztj.sparkcoredemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark01")
    val sc = new SparkContext(conf)

    // 创建 RDD
    // 1) 从内存中创建 makeRDD，底层实现就是 parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)  // 也可以用 Array，设置了 2 个分区
    listRDD.collect().foreach(println)
    println("makeRDD")

    // 2) 从内存中创建 parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4))  // 也可以用 List
    arrayRDD.collect().foreach(println)
    println("parallelize")

    // 3) 由外部存储系统的数据集创建
    // 默认情况下读取项目路径，也可以读取其他路径比如 HDFS
    // val textRDD: RDD[String] = sc.textFile("hdfs://taojie-redmibook:9000/RELEASE")
    // val textRDD: RDD[String] = sc.textFile("in", 2)  指定最少分区数
    val textRDD: RDD[String] = sc.textFile("in")
    textRDD.collect().foreach(println)
    println("textFile")

    // 把 RDD 的数据保存到文件中
    listRDD.saveAsTextFile("out")
  }

}
