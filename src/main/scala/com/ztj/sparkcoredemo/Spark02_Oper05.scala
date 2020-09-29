package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper05 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark02")
    val sc = new SparkContext(conf)

    // foldByKey: aggregateByKey的简化操作，seqop和combop相同，分区内和分区间用同样的函数计算
    val rdd1 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val fold = rdd1.foldByKey(0)(_+_)
    fold.collect().foreach(println)

    // combineByKey: (初始规则, 分区内函数, 分区间函数)
    // 需求：创建一个pairRDD，根据key计算每种key的均值。（先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
    val rdd2 = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    val combine = rdd2.combineByKey(
      (_,1),  // ("a",88) => (88,1)
      (acc:(Int,Int),v)=>(acc._1+v,acc._2+1),  // (88,1), 91 => (179,2)
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2)  // (179,2), (95,1) => (274,3)
    )
    val result = combine.map{case (key,value) => (key,value._1/value._2.toDouble)}
    result.collect().foreach(println)

    // sortByKey
    val rdd3 = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    rdd3.sortByKey(ascending = true).collect().foreach(println)
    rdd3.sortByKey(ascending = false).collect().foreach(println)
  }

}