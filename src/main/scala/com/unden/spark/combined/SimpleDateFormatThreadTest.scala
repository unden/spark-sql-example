package com.unden.spark.combined

import org.apache.spark.sql.SparkSession

object SimpleDateFormatThreadTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleDateFormatThreadTest")
      .master("local")
      .config("spark.sql.warehouse.dir", "hdfs://unden:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val logRDD = spark.sparkContext.textFile("hdfs://unden:9000/data/code-data/access.log")
    logRDD.map(line => {
      val items = line.split("\t")
      val dateStr = items(0)
      DateUtils.format(dateStr)
    }
    ).saveAsTextFile("/home/unden/data/code-data/sdftt")

    spark.stop
  }
}
