package com.ztj.sparkcoredemo

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark02_Oper04 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark02")
    val sc = new SparkContext(conf)

    // partitionBy: 对pairRDD进行分区操作，如果原有的partitionRDD和现有的partitionRDD是一致的话就不进行分区，否则会生成ShuffleRDD，即会产生shuffle过程
    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    val rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
    println(rdd2.partitions.length)
    val rdd3 = rdd1.partitionBy(new MyPartitioner())
    println(rdd3.partitions.length)

    // groupByKey
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
    val group = wordPairsRDD.groupByKey()
    group.collect().foreach(println)
    group.map(t => (t._1, t._2.sum)).collect().foreach(println)

    // reduceByKey: 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置
    //  1. reduceByKey: 按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k,v].\
    //  2. groupByKey: 按照key进行分组，直接进行shuffle
    //  3. reduceByKey比groupByKey更建议使用
    // val reduce = wordPairsRDD.reduceByKey(_+_)
    val reduce = wordPairsRDD.reduceByKey((x, y) => x + y)
    reduce.collect().foreach(println)

    // aggregateByKey: 按key将value进行分组合并，将每个value和初始值作为seq函数的参数，进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算
    // 需求: 创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
    val rdd4 = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    // 查看分区情况
    rdd4.glom().collect().foreach(x=>{
      println(x.mkString(","))
    })
    // (初始值)(分区内seq函数, 分区间combine函数)
    val agg = rdd4.aggregateByKey(0)(math.max, _+_)
    agg.collect().foreach(println)
  }

}

class MyPartitioner() extends Partitioner {
  override def numPartitions: Int = {
    1  // 只有1个分区
  }

  override def getPartition(key: Any): Int = {
    1  // 永远放在第1个分区
  }
}