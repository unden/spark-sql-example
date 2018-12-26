package com.unden.spark.single

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Spark SQL 操作MySQL示例
  */
object MySqlOperate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MySqlOperate")
      .master("local")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://hadoop:3306?characterEncoding=utf8")
      .option("dbtable", "duliday_user.user_inner")
      .option("user", "hive")
      .option("password", "hive")
      .load()

    jdbcDF.show(false)
    val resultDF = jdbcDF.select(jdbcDF.col("create_at"), jdbcDF.col("name"), jdbcDF.col("passwd"))
    resultDF.write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://hadoop:3306?characterEncoding=utf8")
      .option("dbtable", "duliday_user.user_inner_2")
      .option("user", "hive")
      .option("password", "hive")
      .mode(SaveMode.Overwrite)
      .save()

    spark.stop()
  }
}
