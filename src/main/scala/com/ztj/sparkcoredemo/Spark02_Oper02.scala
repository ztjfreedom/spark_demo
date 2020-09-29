package com.ztj.sparkcoredemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark02")
    val sc = new SparkContext(conf)

    // glom算子：将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    val sourceRDD: RDD[Int] = sc.makeRDD(1 to 8,2)
    val glomRDD: RDD[Array[Int]] = sourceRDD.glom()
    glomRDD.collect().foreach(x=>{
      println(x.mkString(","))
    })

    // groupBy：分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器
    val groupByRDD: RDD[(Int, Iterable[Int])] = sourceRDD.groupBy(_ % 2)
    groupByRDD.collect().foreach(println)

    // filter：过滤。返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成
    val filterRDD: RDD[Int] = sourceRDD.filter(_ % 2 == 0)
    filterRDD.collect().foreach(println)

    // sample(withReplacement, fraction, seed)：以指定的随机种子随机抽样出期望比例fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子
    val sampleRDD: RDD[Int] = sourceRDD.sample(withReplacement = false, 0.4, 1)
    sampleRDD.collect().foreach(println)

    // distinct：对源RDD进行去重后返回一个新的RDD。默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它
    // distinct会打乱RDD的分区，会打乱原本的序列顺序
    // 将RDD中一个分区的数据打乱重组到其他不同分区中的操作称为shuffle，没有shuffle的算子性能会快很多
    val dupRDD: RDD[Int] = sc.makeRDD(Array(1, 1, 2, 2, 3, 3), 3)
    val distinctRDD: RDD[Int] = dupRDD.distinct()
    distinctRDD.collect().foreach(println)
    // 用2个分区保存distinct的结果
    dupRDD.distinct(2).collect().foreach(println)

    // coalesce：缩减分区数（接近于合并分区），用于大数据集过滤后，提高小数据集的执行效率
    val rdd: RDD[Int] = sc.parallelize(1 to 16,4)
    println("缩减前", rdd.partitions.length)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)
    println("缩减后", coalesceRDD.partitions.length)

    // repartition：根据分区数，重新通过随机洗牌所有数据
    // repartition实际上是调用的coalesce，默认是进行shuffle的，coalesce默认不shuffle
    val repartitionRDD: RDD[Int] = rdd.repartition(2)
    println(repartitionRDD.glom().collect().foreach(x=>{
      println(x.mkString(","))
    }))
  }

}