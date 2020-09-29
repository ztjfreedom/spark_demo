package com.ztj.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object Streaming05_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming05")
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    val lineDStream = streamingContext.socketTextStream("taojie-redmibook", 9999)

    // 转换
    // lineDStream.map(x => x)
    lineDStream.transform(rdd => rdd)

    lineDStream.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    val wordDStream = lineDStream.flatMap(_.split(" "))
    val wordAndOneDStream = wordDStream.map((_, 1))
    val wordAndCountDStream = wordAndOneDStream.reduceByKey(_+_)
    wordAndCountDStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}