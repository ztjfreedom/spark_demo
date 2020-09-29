package com.ztj.sparkstreaming

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Streaming04_State {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming04")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

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
    val wordsDStream: DStream[(String, Int)] = kafkaDStream.flatMap(_.value().split(" ")).map((_, 1))

    // val wordAndCountDStream = wordsDStream.reduceByKey(_+_)
    // 有状态的数据转换：updateStateByKey
    val stateDStream: DStream[(String, Int)] = wordsDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    stateDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}