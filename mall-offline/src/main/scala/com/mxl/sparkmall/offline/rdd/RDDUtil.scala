package com.mxl.sparkmall.offline.rdd

import com.mxl.sparkmall.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object RDDUtil {
  val logger:Logger = LoggerFactory.getLogger("RDDUtil")

  def userVisitActionRdd(spark: SparkSession): RDD[UserVisitAction] = {
    var sql =
      """
        |select v.*
        |from user_visit_action v,
        |     user_info u
        |where v.user_id=u.user_id and 1=1
      """.stripMargin

    if (!startDate.isEmpty) {
      sql += s" and v.`date` >= '${startDate}'"
    }
    if (!endDate.isEmpty) {
      sql += s" and v.`date` <= '${endDate}'"
    }
    if (!startAge.isEmpty) {
      sql += s" and u.age >= ${startAge}"
    }
    if (!endAge.isEmpty) {
      sql += s" and u.age <= ${endAge}"
    }
    logger.warn(sql)

    import spark.implicits._

    spark.sql(s"use ${HIVE_DATABASE}")
    spark.sql(sql).as[UserVisitAction].rdd
  }


}
