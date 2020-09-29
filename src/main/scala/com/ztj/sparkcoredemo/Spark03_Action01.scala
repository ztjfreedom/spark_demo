package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Action01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark03")
    val sc = new SparkContext(conf)

    // reduce: 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
    val rdd1 = sc.makeRDD(1 to 10,2)
    println(rdd1.reduce(_+_))

    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
    println(rdd2.reduce((x,y)=>(x._1 + y._1, x._2 + y._2)))

    // collect: 在驱动程序中，以数组的形式返回数据集的所有元素
    rdd2.collect().foreach(println)

    // count first
    println(rdd2.count())
    println(rdd2.first())

    // take: 前n个，takeOrdered: 排序后的前n个
    val rdd3 = sc.parallelize(Array(2,5,4,6,8,3))
    rdd3.take(3).foreach(println)
    rdd3.takeOrdered(3).foreach(println)

    // aggregate: 将每个分区里面的元素通过seqOp和初始值(zeroValue)进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作
    println(rdd1.aggregate(0)(_+_, _+_))

    // fold: aggregate的简化操作，seqop和combop一样
    println(rdd1.fold(10)(_+_))
  }

}