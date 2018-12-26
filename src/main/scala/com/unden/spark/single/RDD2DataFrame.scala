package com.unden.spark.single

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RDD2DataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameDemo")
      .master("local")
      .getOrCreate()

    val empRDD = spark.sparkContext.textFile("D:\\imooc\\emp.txt")
    val deptRDD = spark.sparkContext.textFile("D:\\imooc\\dept.txt")

    val empRow = empRDD.map(_.split(","))
      .map(emp => Row(emp(0), emp(1), emp(2), emp(3), emp(4), emp(5), emp(6), emp(7)))

    val deptRow = deptRDD.map(_.split(","))
      .map(dept => Row(dept(0), dept(1), dept(2)))

    val empDF = spark.createDataFrame(empRow, StructType(Array(StructField("empno", StringType, nullable = true),
      StructField("ename", StringType, nullable = true),
      StructField("job", StringType, nullable = true),
      StructField("mgr", StringType, nullable = true),
      StructField("hiredate", StringType, nullable = true),
      StructField("sal", StringType, nullable = true),
      StructField("comm", StringType, nullable = true),
      StructField("deptno", StringType, nullable = true))))

    val deptDF = spark.createDataFrame(deptRow, StructType(Array(StructField("deptno", StringType, nullable = true),
      StructField("dname", StringType, nullable = true),
      StructField("location", StringType, nullable = true))))

    empDF.join(deptDF, empDF.col("deptno") === deptDF.col("deptno"))
      .filter(empDF.col("deptno") === 20)
      .select(empDF.col("empno"), empDF.col("ename"), deptDF.col("location"))
      .orderBy(empDF.col("deptno").asc, empDF.col("empno").desc)
      .limit(2)
      .show(false)

    empDF.groupBy(empDF.col("deptno"))
      .agg(count(empDF.col("empno")).as("numPerDept"))
      .orderBy(empDF.col("deptno").asc)
      .show(false)

    spark.stop()
  }
}
