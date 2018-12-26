package com.unden.spark.single

import org.apache.spark.sql.SparkSession

/**
  * Spark SQL 操作csv文件示例
  */
object CsvOperate {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: JsonFileOperate <inputPath> <outputPath>")
      System.exit(0)
    }

    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop:9000/user/hive/warehouse")
      .appName("CsvOperate")
      .master("local")
      .getOrCreate()

    val csvDF = spark.read.format("csv").load(args(0))
    csvDF.write.format("csv").save(args(1))

    spark.stop()
  }
}
