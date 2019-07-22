package com.mxl.sparkmall.offline.app

import com.mxl.sparkmall.common.bean.UserVisitAction
import com.mxl.sparkmall.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mxl.sparkmall.common._

//需求2：Top10热门品类中每个品类的 Top10 活跃 Session 统计
object CategoryTop10SessionIdTop10 {
  def top10CategorySession(spark: SparkSession, top10Categorys: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String): Unit = {
    val top10CategoryIds: List[String] = top10Categorys.map(category => category.categoryId)

    //过滤出点击了top10品类的rdd
    var filterRdd: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => top10CategoryIds.contains(uva.click_category_id.toString))

    val categoryTop10SessionIds: RDD[(String, List[(String, Int)])] =
      filterRdd.map(uva => ((uva.click_category_id.toString, uva.session_id), 1)) //转化为((cid,sid),1)
        .reduceByKey(_ + _) //转化为((cid,sid）,count)
        .map({ case ((cid, sid), count) => (cid, (sid, count)) }) //转化为(cid,(sid,count))
        .groupByKey().map({ case (cid, iterator) =>
        (cid, iterator.toList.sortBy(-_._2).take(10))
      }) //分组、降序、取前10

    //封装到样例类
    val resultRdd: RDD[CategorySession] = categoryTop10SessionIds.flatMap({
      case (cid, iterator) => iterator.map({
        case (sid, count) => CategorySession(taskId, cid, sid, count)
      })
    })
    import spark.implicits._

    //    val sql = "insert into category_top10_session_count values(?,?,?,?)"
    //    var args: RDD[Array[Any]] = resultRdd.map(cs => {
    //      Array[Any](cs.taskId,cs.categoryId,cs.sessionId,cs.clickCount)
    //    })
    //    JDBCUtil.executeBatchUpdate(sql, args)

    //通过df写入sql
    resultRdd.toDF().write.format("jdbc")
      .option("url", JDBC_URL)
      .option("user", JDBC_USER)
      .option("password", JDBC_PASSWORD)
      .option("dbtable", "category_top10_session_count")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
