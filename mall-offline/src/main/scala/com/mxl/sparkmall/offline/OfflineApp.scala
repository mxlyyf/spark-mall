package com.mxl.sparkmall.offline

import com.mxl.sparkmall.offline.rdd.RDDUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object OfflineApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("OfflineApp")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/sparkmall")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //RDDUtil.userVisitActionRdd(spark).collect()

  }
}
