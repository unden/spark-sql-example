package com.unden.spark.single

import org.apache.spark.sql.SparkSession

/**
  * Spark SQL 操作parquet文件示例
  */
object ParquetFileOperate {

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

    val parquetDF = spark.read.format("parquet").load(args(0))
    parquetDF.write.format("parquet").save(args(1))

    spark.stop()
  }

}
