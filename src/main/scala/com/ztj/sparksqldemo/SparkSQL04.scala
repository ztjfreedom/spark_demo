package com.ztj.sparksqldemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object SparkSQL04 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    // 读取
    // sparkSession.read.json("in/user.json").show()
    val df = sparkSession.read.load("in/users.parquet")

    // 保存，默认是保存成parquet文件
    // df.write.save("output")
    df.write.format("json").mode("append").save("output")

    sparkSession.stop()
  }

}