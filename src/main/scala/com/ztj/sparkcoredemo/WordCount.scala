package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    // 2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    // 3.使用sc创建RDD并执行相应的transformation和action
    // 路径会从默认的部署环境中查找，比如yarn
    // 如果需要本地查找: file:////...
    val result = sc.textFile("in").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect()
    // val result = sc.textFile("file:////opt/spark/spark-2.4.6-bin-hadoop2.7/input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect()
    result.foreach(println)

    // 4.关闭连接
    sc.stop()
  }

}
