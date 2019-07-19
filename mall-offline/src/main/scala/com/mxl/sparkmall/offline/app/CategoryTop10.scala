package com.mxl.sparkmall.offline.app

import java.util.UUID

import com.mxl.sparkmall.common.bean.UserVisitAction
import com.mxl.sparkmall.common.util.JDBCUtil
import com.mxl.sparkmall.offline.accu.StaticsAccu1
import com.mxl.sparkmall.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTop10 {
  def staticsTop10Categorys(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction],taskId: String)  = {
    val acc: StaticsAccu1 = new StaticsAccu1
    spark.sparkContext.register(acc) //注册累加器

    userVisitActionRDD.foreach(action => {
      acc.add(action)
    })

    //按点击数、下单数、支付数降序排序
    val list: List[(String, (Long, Long, Long))] = acc.value.toList.sortBy({
      case (_, (clickCount, orderCount, payCount)) => (-clickCount, -orderCount, -payCount)
    }).take(10)

    //将结果封装到样例类中
    val top10Categorys: List[CategoryCountInfo] = list.map({
      case (cid, (clickCount, orderCount, payCount)) =>
        CategoryCountInfo(taskId, cid, clickCount, orderCount, payCount)
    })

    //将统计结果写入mysql
    val sql = "insert into category_top10 values(?, ?, ?, ?, ?)"
    val args: List[Array[Any]] = top10Categorys.map(category => {
      Array[Any](category.taskId, category.categoryId, category.clickCount, category.orderCount, category.payCount)
    })
    JDBCUtil.executeUpdate("truncate category_top10", null)
    JDBCUtil.executeBatchUpdate(sql,args)

    top10Categorys
  }
}
