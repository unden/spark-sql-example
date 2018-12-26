package com.unden.spark.combined

import org.apache.spark.sql.SparkSession

object MySqlJoinHiveOperate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameDemo")
      .master("local")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val empFromHive = spark.table("emp")

    val deptFromMysql = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://hadoop:3306")
      .option("dbtable", "imooc.dept")
      .option("user", "hive")
      .option("password", "hive")
      .load()

    empFromHive.join(deptFromMysql, empFromHive.col("deptno") === deptFromMysql.col("deptno"))
      .show(false)

    spark.stop()
  }
}
