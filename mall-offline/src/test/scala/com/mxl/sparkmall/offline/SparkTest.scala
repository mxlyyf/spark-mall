package com.mxl.sparkmall.offline

import com.mxl.sparkmall.offline.rdd.RDDUtil
import com.mxl.sparkmall.offline.udaf.CityRemarkUDAF
import org.apache.spark.sql.SparkSession
import org.junit
import org.junit.{After, Before}

class SparkTest {
  var spark: SparkSession = _

  @Before
  def befor: Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("JunitTest2")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/sparkmall")
      .getOrCreate()
  }

  @After
  def after: Unit = {
    spark.stop()
  }

  @junit.Test
  def test01: Unit = {
    spark.sql("use sparkmall")
    spark.sql("select count(*) from user_visit_action").show()

  }

  @junit.Test
  def test02: Unit = {
    RDDUtil.userVisitActionRdd(spark).foreach(println)
  }

  @junit.Test
  def test05: Unit = {
    spark.sql("use sparkmall")

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
    spark.sql(sql).show(30)

  }
}
