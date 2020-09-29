package com.ztj.sparksqldemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL02 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL02")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // rdd和dataframe的转换需要引入
    import sparkSession.implicits._

    // rdd转为dataframe
    val rdd = sparkSession.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))
    val df = rdd.toDF("id", "name", "age")

    // 转为dataset
    val ds = df.as[User]
    ds.show()

    // 转为dataframe
    val df1 = ds.toDF()

    // 转为rdd
    val rdd1 = df1.rdd
    rdd1.foreach(row => {
      println(row.getString(1))  // 下标从0开始
    })

    // rdd直接转换为dataset
    val userRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS = userRDD.toDS()
    userDS.show()

    // dataset直接转换为rdd
    val rdd2 = userDS.rdd
    rdd2.foreach(println)

    sparkSession.stop()
  }

}

case class User(id:Int, name:String, age:Int)