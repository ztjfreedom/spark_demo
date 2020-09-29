package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Dependency {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark04")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6))
    val mapRDD = rdd.map((_, 1))
    val reduceRDD = mapRDD.reduceByKey(_+_)
    println(reduceRDD.toDebugString)
    println(reduceRDD.dependencies)
  }

}