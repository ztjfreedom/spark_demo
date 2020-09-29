package com.ztj.sparkcoredemo

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_ShareData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark05")
    val sc = new SparkContext(conf)

    val dataRDD = sc.makeRDD(List(1,2,3,4), 2)

    // 使用累加器来共享变量
    val accumulator = sc.longAccumulator
    dataRDD.foreach {
      case i => {
        accumulator.add(i)
      }
    }
    println("sum = " + accumulator.value)

    // 创建自定义累加器
    val wordAccumulator = new WordAccumulator
    sc.register(wordAccumulator)

    val rdd = sc.makeRDD(List("hadoop", "hive", "spark"), 2)
    rdd.foreach {
      case w => {
        wordAccumulator.add(w)
      }
    }
    println(wordAccumulator.value)

    // 广播
    val rdd1 = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    // val rdd2 = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    // val joinRDD = rdd1.join(rdd2)
    // joinRDD.foreach(println)
    // join涉及到shuffle效率比较低下，使用广播来替代join操作
    val list = List((1, 1), (2, 2), (3, 3))

    // 广播可以减少数据的传输
    val broadcast = sc.broadcast(list)
    val resultRDD = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRDD.foreach(println)

    sc.stop()
  }

}

// 自定义累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()

  override def isZero: Boolean = list.isEmpty

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  override def reset(): Unit = list.clear()

  // 累加运算
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  // 合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = list
}