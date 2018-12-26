package com.unden.spark.single

import com.unden.spark.domain.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * RDD转化为DataFrame的两种方式
  */
object RDD2DF {

  def convertByReflection(infoRDD: RDD[String], spark: SparkSession) = {
    import spark.implicits._
    infoRDD.map(_.split(","))
      .map(attributes => Person(attributes(0).toLong, attributes(1), attributes(2).toInt))
      .toDF
  }

  def convertByProgrammatically(infoRDD: RDD[String], spark: SparkSession) = {
    val infoRddRow = infoRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2)))
    spark.createDataFrame(infoRddRow, StructType(Array(StructField("id", StringType),
      StructField("name", StringType),
      StructField("age", StringType))))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD2DF")
      .master("local")
      .config("spark.sql.warehouse.dir", "hdfs://unden:9000/user/hive/warehouse")
      .getOrCreate

    val infoRDD = spark.sparkContext.textFile("/home/unden/data/code-data/people.txt")

//    val infoDF = convertByReflection(infoRDD, spark);
    val infoDF = convertByProgrammatically(infoRDD, spark);
    infoDF.printSchema
    infoDF.show()

    spark.stop
  }
}