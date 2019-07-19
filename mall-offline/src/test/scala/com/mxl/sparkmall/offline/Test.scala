package com.mxl.sparkmall.offline

import com.mxl.sparkmall.offline.rdd.RDDUtil
import org.apache.spark.sql.SparkSession
import org.junit
import org.junit.{After, Before}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.HashMap
import scala.collection.mutable

class Test{
  val logger:Logger = LoggerFactory.getLogger(classOf[Test])

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
  def test03: Unit ={
    var map: Map[String, (Long, Long, Long)] = new HashMap[String, (Long, Long, Long)]

    map += "1" -> (1,1,1)
    map += "2" -> (2,2,2)

    map += "1" -> (2,3,1)
    println(map.size)
    map.foreach(println)
  }

  @junit.Test
  def test04: Unit ={
    var list: List[String] = List("a","b","c","d")

    println(list.contains("b"))
  }

  @junit.Test
  def test05: Unit ={
    spark.sql("use sparkmall")

    val sql = """select
                | t3.area,
                | t4.product_name,
                | t3.click_count,
                | t3.rk
                |from
                |(
                |select
                |	t2.area,
                |	t2.click_product_id,
                |	t2.click_count,
                |	dense_rank() over( partition by t2.area order by t2.click_count desc) rk
                |from
                |	(
                |		select t1.area,t.click_product_id,count(t.click_product_id) click_count
                |			from user_visit_action t,city_info t1
                |			where t.click_product_id > -1 and t.city_id=t1.city_id
                |			group by t1.area,t.click_product_id
                |	) t2 ) t3,
                |	product_info t4
                |	where t4.product_id = t3.click_product_id and rk <= 3""".stripMargin
    spark.sql(sql).show(30)

  }
}

