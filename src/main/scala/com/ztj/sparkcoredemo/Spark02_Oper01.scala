package com.ztj.sparkcoredemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark02")
    val sc = new SparkContext(conf)

    // map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 5, 2)
    // val mapRDD: RDD[Int] = listRDD.map(x => x * 2)  下方为简化写法
    val mapRDD: RDD[Int] = listRDD.map(_ * 2)
    mapRDD.collect().foreach(println)

    // mapPartition算子
    // 独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U]。假设有N个元素，有M个分区，那么map的函数的将被调用N次，而mapPartitions被调用M次，一个函数一次处理所有分区
    // mapPartition效率优于map，减少了发送到执行器的交互次数
    // 但是mapPartition可能会出现内存溢出问题
    val mapPartitionRDD: RDD[Int] = listRDD.mapPartitions(data => {
      data.map(x => x * 2)}  // 这里的map是Scala中的map，不是Spark RDD中的map
    )
    mapPartitionRDD.collect().foreach(println)

    // mapPartitionsWithIndex算子
    // mapPartitionsWithIndex算子能够保持分区号
    val mapPartitionWithIndexRDD: RDD[(Int, Int)] = listRDD.mapPartitionsWithIndex((index, data) => {
      data.map(x => {(index, x * 2)})
    })
    mapPartitionWithIndexRDD.collect().foreach(println)

    // flatMap算子
    // 类似于map，先映射后扁平化
    val sourceRDD: RDD[String] = sc.parallelize(List("coffee panda","happy panda","happiest panda party"))
    val flatMapRDD: RDD[String] = sourceRDD.flatMap(x=>x.split(" "))
    flatMapRDD.collect().foreach(println)
  }

}