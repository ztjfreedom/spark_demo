package com.ztj.sparkstreaming

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming03_Kafka {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming03")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

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
    val words = kafkaDStream.flatMap(_.value().split(" ")).map((_, 1))
    val wordAndCountDStream = words.reduceByKey(_+_)
    wordAndCountDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}