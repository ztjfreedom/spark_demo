package com.ztj.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object Streaming01_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming01")

    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 通过监控端口创建DStream，读进来的为一行数据
    val lineDStream = streamingContext.socketTextStream("taojie-redmibook", 9999)

    // 将每一行数据做切分，形成一个个单词
    val wordDStream = lineDStream.flatMap(_.split(" "))

    // 将单词映射成元组（word,1）
    val wordAndOneDStream = wordDStream.map((_, 1))

    // 将相同的单词次数做统计
    val wordAndCountDStream = wordAndOneDStream.reduceByKey(_+_)

    wordAndCountDStream.print()

    // 启动SparkStreamingContext，启动采集器
    streamingContext.start()
    streamingContext.awaitTermination()

    // 采集程序一般不需要停止
    // streamingContext.stop()
  }

}