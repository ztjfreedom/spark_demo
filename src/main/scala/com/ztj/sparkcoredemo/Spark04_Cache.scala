package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Cache {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark04")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array("abc"))
    val nocache = rdd.map(_ + System.currentTimeMillis)
    nocache.collect().foreach(println)
    nocache.collect().foreach(println)

    val cache =  rdd.map(_ + System.currentTimeMillis).cache
    cache.collect().foreach(println)
    cache.collect().foreach(println)
  }

}