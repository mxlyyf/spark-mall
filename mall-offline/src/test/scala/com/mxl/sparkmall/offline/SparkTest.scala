package com.mxl.sparkmall.offline

import com.mxl.sparkmall.common.bean.UserSaleDetailDayCount
import com.mxl.sparkmall.common.util.ESUtil
import com.mxl.sparkmall.offline.rdd.RDDUtil
import com.mxl.sparkmall.offline.udaf.CityRemarkUDAF
import io.searchbox.client.JestClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit
import org.junit.{After, Before}

class SparkTest {
  var spark1: SparkSession = _

  @Before
  def befor: Unit = {
    spark1 = SparkSession
      .builder()
      .master("local[2]")
      .appName("SparkTest")
      .enableHiveSupport()
      //.config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/sparkmall")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/warehouse/gmall/")
      .getOrCreate()

  }

  @After
  def after: Unit = {
    spark1.stop()
  }

  @junit.Test
  def test01: Unit = {
    spark1.sql("use sparkmall")
    spark1.sql("select count(*) from user_visit_action").show()

  }

  @junit.Test
  def test02: Unit = {
    RDDUtil.userVisitActionRdd(spark1).foreach(println)
  }

  @junit.Test
  def test05: Unit = {
    spark1.sql("use sparkmall")

    spark1.udf.register("city_remark", new CityRemarkUDAF)

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
    spark1.sql(sql).show(30)
  }

  //测试： 把hive表dws_user_sale_detail_daycount的数据导入ES
  @junit.Test
  def hiveToESTest() = {
    val spark2 = SparkSession
      .builder()
      .master("local[2]")
      .appName("SparkTest2")
      .enableHiveSupport()
      //.config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/sparkmall")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/warehouse/gmall/")
      .getOrCreate()

    val date = "2019-02-10" //将改天的数据导入ES
    val index = "sparkmall_sale_detail"  //ES对应Index
    val sql =
      s"""
         |select
         |    user_id,
         |    sku_id,
         |    user_gender,
         |    cast(user_age as int) user_age,
         |    user_level,
         |    cast(nvl(order_price,0) as double) order_price,
         |    sku_name,
         |    sku_tm_id,
         |    sku_category3_id,
         |    sku_category2_id,
         |    sku_category1_id,
         |    sku_category3_name,
         |    sku_category2_name,
         |    sku_category1_name,
         |    spu_id,
         |    sku_num,
         |    cast(order_count as bigint) order_count,
         |    cast(order_amount as double) order_amount,
         |    dt
         |from dws_sale_detail_daycount
         |where dt='$date'
        """.stripMargin
    import spark2.implicits._

    spark2.sql("use gmall")
    val ds: Dataset[UserSaleDetailDayCount] = spark1.sql(sql).as[UserSaleDetailDayCount]
    ds.foreachPartition(it => {
      val list: List[UserSaleDetailDayCount] = it.toList
      //val jestClient: JestClient = ESUtil.getJest()
      ESUtil.insertBulk(index, list)

    })
    spark2.stop()
  }
}
