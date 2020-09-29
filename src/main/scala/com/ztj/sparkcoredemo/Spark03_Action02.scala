package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Action02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark03")
    val sc = new SparkContext(conf)

    // save
    // val rdd1 = sc.makeRDD(Array(("a",1),("b",2),("c",3)))
    // rdd1.saveAsTextFile("out1")
    // rdd1.saveAsSequenceFile("out2")
    // rdd1.saveAsObjectFile("out3")

    // countByKey
    val rdd2 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    println(rdd2.countByKey())

    // foreach: collect之后返回的是array，collect之后foreach使用的是scala的函数，在Driver中执行
    // rdd的foreach由spark提供，在Executor中执行
    rdd2.foreach(println(_))
  }

}