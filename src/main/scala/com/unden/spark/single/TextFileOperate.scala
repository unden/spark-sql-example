package com.unden.spark.single

import org.apache.spark.sql.SparkSession

/**
  * Spark SQL 操作普通文本文件示例
  */
object TextFileOperate {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: TextFileOperate <inputPath> <outputPath>")
      System.exit(0)
    }

    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "hdfs://unden:9000/user/hive/warehouse")
      .appName("TextFileOperate")
      .master("local")
      .getOrCreate()

    val peopleRDD = spark.sparkContext.textFile(args(0))
    peopleRDD.foreach(println)
    peopleRDD.saveAsTextFile(args(1))

    spark.stop()
  }

}
