package com.ztj.sparksqldemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取文件生成dataframe
    val df = sparkSession.read.json("in/user.json")
    df.show()

    // 采用sql访问数据
    df.createOrReplaceTempView("user")
    sparkSession.sql("select * from user").show()

    sparkSession.stop()
  }

}
