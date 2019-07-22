package com.mxl.sparkmall.realtime.app

import com.mxl.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

//需求7：每天各地区各城市热门广告Top3
////areaCityAdClickCountPerDayDstream 来自需求6dstream
object AreaHotAdTop3PerDay {
  //hash类型
  val key = "day:area:ad:top3"

  def areaHotAdTop3PerDay(areaCityAdClickCountPerDayDstream: DStream[(String, Int)]): Unit = {
    //("day:area:city:adid",count) -> ("day:area:adid",count)
    val areaAdClickCountPerDayDstream: DStream[(String, Int)] = areaCityAdClickCountPerDayDstream.map {
      case (field, count) =>
        val params: Array[String] = field.split(":")
        (s"${params(0)}:${params(1)}:${params(3)}", count)
    }.reduceByKey(_ + _)

    //("day:area:adid",count) -> ("day:area",(adid,count)) ->("day:area",Iterable[(adid,count)])
    val dayAreaItDstream: DStream[(String, Iterable[(String, Int)])] = areaAdClickCountPerDayDstream.map {
      case (field, count) =>
        val params: Array[String] = field.split(":")
        val ct = params.length
        val a = 0
        (s"${params(0)}:${params(1)}", (params(2), count))
    }.groupByKey()

    //排序取top3
    val resultDstream: DStream[(String, List[(String, Int)])] = dayAreaItDstream.map {
      case (field, it) =>
        val top3List: List[(String, Int)] = it.toList.sortWith(_._2 > _._2).take(3)
        (field, top3List)
    }
    resultDstream.print()

    //写入redis
    resultDstream.foreachRDD(rdd => {
      rdd.foreachPartition {
        case it =>
          val jedis = RedisUtil.getJedis
          it.foreach { case (field, list) =>
            //scala Map 转 java.util.Map
            import scala.collection.JavaConversions.mapAsJavaMap

            // 把list转换json字符串
            //fastjson 对java的数据结构支持的比较, 对scala独有的结构支持的不好
            import org.json4s.JsonDSL._ // 提供隐式转换
            jedis.hset(key,field,JsonMethods.compact(JsonMethods.render(list)))
          }
          jedis.close()
      }
    })

  }
}
