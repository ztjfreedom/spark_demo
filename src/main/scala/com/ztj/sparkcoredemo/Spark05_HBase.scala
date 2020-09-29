package com.ztj.sparkcoredemo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_HBase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark05")
    val sc = new SparkContext(sparkConf)

    // 构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()
    // conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    conf.set(TableInputFormat.INPUT_TABLE, "user")

    // 从HBase读取数据形成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count: Long = hbaseRDD.count()
    println(count)

    // 对hbaseRDD进行处理
    hbaseRDD.foreach {
      case (_, result) =>
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        val color: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("color")))
        println("RowKey:" + key + ",Name:" + name + ",Color:" + color)
    }

    // 向HBase写入
    val dataRDD = sc.makeRDD(List(("1002", "zhangsan"), ("1003", "lisi")))
    val putRDD = dataRDD.map {
      case (rowkey, name) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),Bytes.toBytes(name))
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "user")

    putRDD.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}