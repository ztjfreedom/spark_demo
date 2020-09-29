package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_CheckPoint {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark04")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRDD = rdd.map((_, 1))

    // sc.setCheckpointDir("hdfs://hadoop102:9000/checkpoint")
    sc.setCheckpointDir("cp")
    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.foreach(println)

    println(reduceRDD.toDebugString)

    sc.stop()
  }

}