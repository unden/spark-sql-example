package com.unden.spark.single

import org.apache.spark.sql.SparkSession

object HiveOperate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameDemo")
      .master("local")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val empHive = spark.table("emp")

    empHive.show(false)
    empHive.write.saveAsTable("emp")
    spark.stop()
  }
}
