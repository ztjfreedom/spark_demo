package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper06 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark02")
    val sc = new SparkContext(conf)

    // mapValues: 针对于(K,V)形式的类型只对V进行操作
    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    rdd1.mapValues(_+"0").collect().foreach(println)

    // join: 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
    val rdd2 = sc.parallelize(Array((1,4),(2,5),(3,6)))
    rdd1.join(rdd2).collect().foreach(println)

    // cogroup: 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
    // join必须要两个RDD中都有的key才会存在在结果中
    // cogroup即使一个RDD中不存在key也会被列出来
    rdd1.cogroup(rdd2).collect().foreach(println)
  }

}