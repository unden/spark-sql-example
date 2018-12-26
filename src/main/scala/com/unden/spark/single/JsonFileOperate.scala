package com.unden.spark.single

import org.apache.spark.sql.SparkSession

/**
  * Spark SQL 操作json文件示例
  */
object JsonFileOperate {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: JsonFileOperate <inputPath> <outputPath>")
      System.exit(0)
    }

    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop:9000/user/hive/warehouse")
      .appName("TextFileOperate")
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.format("json").load(args(0))
    jsonDF.write.format("json").save(args(1))

    spark.stop()
  }

}
