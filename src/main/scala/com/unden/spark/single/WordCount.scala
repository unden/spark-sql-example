package com.unden.spark.single

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("yarn-client")

    val sc = new SparkContext(conf)
    val fileRDD = sc.textFile("file:/home/unden/data/code-data/people.txt")
    fileRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)

    sc.stop()
  }

}
