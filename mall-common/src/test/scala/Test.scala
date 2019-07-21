import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit
import org.junit._
import com.mxl.sparkmall.common._
import com.mxl.sparkmall.common.util.{RandomUtil, RedisUtil}

class Test extends Assert {

  var spark: SparkSession = _

  //@Before
  def befor: Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("JunitTest")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/sparkmall")
      .getOrCreate()
  }

  //@After
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
    println(RandomUtil.getRandomDate("2019-07-10", "2019-07-18"))
    println(RandomUtil.getRandomDate("2019-07-10", "2019-07-18"))
    println(RandomUtil.getRandomDate("2019-07-10", "2019-07-18"))
    println(RandomUtil.getRandomDate("2019-07-10", "2019-07-18"))
  }

  @junit.Test
  def test03: Unit ={
    println(RedisUtil.getJedis.smembers("blacklist"))
  }
}

