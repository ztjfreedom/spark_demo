package com.ztj.sparkcoredemo

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_MySQL {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark05")
    val sc = new SparkContext(conf)

    // 定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    // 创建JdbcRDD查询数据
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from `user` where `id`>=?;",
      1,
      10,
      1,
      r => (r.getInt(1), r.getString(2))
    )

    // 打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    // 保存数据到mysql
    val dataRDD = sc.makeRDD(List(("zhangsan", 10), ("lisi", 20)))

    dataRDD.foreachPartition(datas => {
      // 数据库连接对象每个分区1个
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, userName, passWd)
      datas.foreach {
        case (username, age) => {
          val sql = "insert into user(name, age) values(?, ?)"
          val statement = connection.prepareStatement(sql)
          statement.setString(1, username)
          statement.setInt(2, age)
          statement.executeUpdate()
          statement.close()
        }
      }
      connection.close()
    })
  }

}