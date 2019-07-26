import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit
import org.junit._
import com.mxl.sparkmall.common._
import com.mxl.sparkmall.common.bean.StartupLog
import com.mxl.sparkmall.common.util.{ESUtil, RandomUtil, RedisUtil}

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

  //测试：ES计数
  @junit.Test
  def test04(): Unit ={
    println(ESUtil.count("sparkmall_dau"))
  }

  //测试：ES插入数据
  @junit.Test
  def test05(): Unit ={
    val source1 = StartupLog("mid777111", "uid222", "", "", "", "", "", "", 123124141)
    val source2 = StartupLog("mid22222", "uid101", "", "", "", "", "", "", 129489124)
    //StartupLog(mid_72,4115,gmall,shanghai,android,360,null,1.2.0,1564143817294)
    ESUtil.insertBulk("sparkmall_dau", source1 :: source2 :: Nil)
  }
}

