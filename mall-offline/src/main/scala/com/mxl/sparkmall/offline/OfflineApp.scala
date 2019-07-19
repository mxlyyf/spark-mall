package com.mxl.sparkmall.offline

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object OfflineApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("JunitTest")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/sparkmall")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    spark.sql("")

  }
}
