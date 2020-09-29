package com.ztj.sparksqldemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object SparkSQL03_UDF {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    val rdd = sparkSession.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))
    val df = rdd.toDF("id", "name", "age")
    df.createOrReplaceTempView("user")

    // user defined function 自定义函数
    sparkSession.udf.register("addName", (x: String) => "name:" + x)
    sparkSession.sql("select addName(name), age from user").show()

    // 自定义聚合函数（弱类型）
    sparkSession.udf.register("avgAge", new MyAvg)
    sparkSession.sql("select avgAge(age) from user").show()

    // 自定义聚合函数（强类型）
    val udaf = new MyAvgClass
    val avgCol = udaf.toColumn.name("avgAgeClass")
    val ds = df.as[User]
    ds.select(avgCol).show()

    sparkSession.stop()
  }

}

// 自定义聚合函数（弱类型）
class MyAvg extends UserDefinedAggregateFunction {
  // 输入数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 计算时数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 返回数据结构
  override def dataType: DataType = DoubleType

  // 函数是否稳定
  override def deterministic: Boolean = true

  // 计算前初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L  // sum
    buffer(1) = 0L  // count
  }

  // 根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)  // sum
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)  // count
  }

  // 计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

// 自定义聚合函数（强类型）
// case class User(id: Int, name: String, age: Int)  已在别处定义
case class AvgBuffer(var sum: Int, var count: Int)
class MyAvgClass extends Aggregator[User, AvgBuffer, Double] {
  // 初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  // 累加，更新
  override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 多个节点的缓冲区合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // 自定义类型
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
  // 默认类型
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}