package com.ztj.sparkcoredemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Serializable {

  def main(args: Array[String]): Unit = {

    // 1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark04").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // 2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    // 3.创建一个Search对象
    val search = new Search("h")

    // 4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatch1(rdd)
    match1.collect().foreach(println)
  }

}

// 必须要序列化
// 初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的，这就涉及到了跨进程通信，是需要序列化的
class Search(query:String) extends Serializable {
  // 过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 过滤出包含字符串的RDD，这里使用了Search对象的成员方法，所以需要序列化
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  // 过滤出包含字符串的RDD，这里传递了query属性，也需要序列化
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }

}