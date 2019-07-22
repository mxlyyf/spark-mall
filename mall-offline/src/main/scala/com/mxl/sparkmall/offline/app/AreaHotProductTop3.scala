package com.mxl.sparkmall.offline.app

import com.mxl.sparkmall.common.{JDBC_PASSWORD, JDBC_URL, JDBC_USER}
import com.mxl.sparkmall.offline.udaf.CityRemarkUDAF
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//需求4：各区域热门商品（点击量）Top3
object AreaHotProductTop3 {

  def statisticAreaHotProductTop3(spark: SparkSession): Unit = {
    spark.sql("use sparkmall")
    //注册udaf
    spark.udf.register("city_remark", new CityRemarkUDAF)

    val sql =
      """select
        | t3.area,
        | t4.product_name,
        | t3.click_count,
        | t3.rk,
        | t3.remark
        |from
        |(
        |select
        |	t2.area,
        |	t2.click_product_id,
        |	t2.click_count,
        | t2.remark,
        |	dense_rank() over( partition by t2.area order by t2.click_count desc) rk
        |from
        |	(
        |		select
        |     t1.area,
        |     t.click_product_id,
        |     count(t.click_product_id) click_count,
        |     city_remark(t1.city_name) remark
        |		from user_visit_action t,city_info t1
        |		where t.click_product_id > -1 and t.city_id=t1.city_id
        |		group by t1.area,t.click_product_id
        |	) t2 ) t3,
        |	product_info t4
        |	where t4.product_id = t3.click_product_id and rk <= 3""".stripMargin

    val areaHotProductTop3Dframe: DataFrame = spark.sql(sql)
    areaHotProductTop3Dframe.show

    //保存到mysql
    areaHotProductTop3Dframe.write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url", JDBC_URL)
      .option("user", JDBC_USER)
      .option("password", JDBC_PASSWORD)
      .option("dbtable", "area_hotproduct_top3")
      .save()
  }

}
