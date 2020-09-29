package com.ztj.sparkcoredemo

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

object Spark05_JSON {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark05")
    val sc = new SparkContext(conf)

    // 文件要符合Spark对JSON文件的要求：每一行就是一个JSON记录
    val json = sc.textFile("in/user.json")
    val result  = json.map(JSON.parseFull)
    result.foreach(println)
  }

}