package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper03 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark02")
    val sc = new SparkContext(conf)

    // sortBy
    val rdd1 = sc.parallelize(List(2,1,3,4))
    rdd1.sortBy(x => x).collect().foreach(println)
    // rdd1.sortBy(x => x, ascending = false).collect()  // 降序
    // rdd1.sortBy(x => x%3).collect()  // 按照除以3余数的大小排序

    // union
    val rdd2 = sc.makeRDD(1 to 3)
    val rdd3 = sc.makeRDD(2 to 4)
    rdd2.union(rdd3).collect().foreach(println)

    // subtract: 计算差的一种函数，去除两个RDD中相同的元素，不同的RDD将保留下来
    rdd2.subtract(rdd3).collect().foreach(println)

    // intersection: 求交集
    rdd2.intersection(rdd3).collect().foreach(println)

    // cartesian: 笛卡尔积（尽量避免使用）
    rdd2.cartesian(rdd3).collect().foreach(println)

    // zip: 将两个RDD组合成Key/Value形式的RDD，这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常
    rdd2.zip(rdd3).collect().foreach(println)
  }

}