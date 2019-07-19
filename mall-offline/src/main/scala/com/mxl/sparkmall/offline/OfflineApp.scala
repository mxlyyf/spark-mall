package com.mxl.sparkmall.offline

import java.util.UUID

import com.mxl.sparkmall.common.bean.UserVisitAction
import com.mxl.sparkmall.offline.app.CategoryTop10._
import com.mxl.sparkmall.offline.app.CategoryTop10SessionIdTop10
import com.mxl.sparkmall.offline.bean.CategoryCountInfo
import com.mxl.sparkmall.offline.rdd.RDDUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
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
    sc.setCheckpointDir("hdfs://192.168.213.101:9000/spark-checkpoint") //设置持久化路径

    val userVisitActionRDD: RDD[UserVisitAction] = RDDUtil.userVisitActionRdd(spark)
    userVisitActionRDD.cache() //缓存RDD
    val taskId = UUID.randomUUID().toString.replaceAll("-","")

    //需求1：按照每个品类的 点击、下单、支付 的量来统计热门品类
    val top10categorys: List[CategoryCountInfo] = staticsTop10Categorys(spark, userVisitActionRDD, taskId)

    //需求2：Top10热门品类中每个品类的 Top10 活跃 Session 统计
    CategoryTop10SessionIdTop10.top10CategorySession(spark,top10categorys,userVisitActionRDD,taskId)

    //需求3：

    //需求4：

    //需求5：

    //需求6：

  }
}
