package com.ztj.sparkstreaming

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Streaming04_Window {

  def main(args: Array[String]): Unit = {
    val ints = List(1,2,3,4,5,6)
    val slides = ints.sliding(3, 2)
    for (slide <- slides) {
      println(slide.mkString(","))
    }

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming04")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val streamingContext = new StreamingContext(sparkContext, Seconds(3))

    // 保存数据状态需要设置检查点路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    // 配置kafka的参数
    val brokers: String = "127.0.0.1:9092"
    val topics = Array("sparkstream")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> "sparkstream",
      "key.serializer" -> classOf[StringSerializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
    )

    // Kafka DStream
    val kafkaDStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // 窗口，窗口大小应为采集周期的整数倍，滑动步长也应为采集周期的整数倍
    val inputDStream = kafkaDStream.map(_.value())
    val windowDStream = inputDStream.window(Seconds(9), Seconds(3))

    val wordsDStream = windowDStream.flatMap(_.split(" ")).map((_, 1))
    val wordAndCountDStream = wordsDStream.reduceByKey(_+_)

    wordAndCountDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}