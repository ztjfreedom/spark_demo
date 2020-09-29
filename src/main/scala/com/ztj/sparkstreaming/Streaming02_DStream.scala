package com.ztj.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming02_DStream {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming02")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 文件系统采集器：从指定文件夹中采集数据
    // val lineDStream = streamingContext.textFileStream("stream")

    // 自定义采集器
    val lineDStream = streamingContext.receiverStream(new MyReceiver("taojie-redmibook", 9999))

    val wordDStream = lineDStream.flatMap(_.split(" "))
    val wordAndOneDStream = wordDStream.map((_, 1))
    val wordAndCountDStream = wordAndOneDStream.reduceByKey(_+_)
    wordAndCountDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

// 自定义采集器
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  var socket: java.net.Socket = _

  def receive(): Unit = {
    socket = new java.net.Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    while (true) {
      val line: String = reader.readLine()
      if (!line.isEmpty) {
        if ("END".equals(line)) {
          return
        } else {
          this.store(line)
        }
      }
    }
  }

  override def onStart(): Unit = {
    new Thread(() => {
      receive()
    }).start()
  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}