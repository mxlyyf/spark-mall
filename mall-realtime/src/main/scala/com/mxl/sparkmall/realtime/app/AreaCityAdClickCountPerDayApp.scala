package com.mxl.sparkmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.mxl.sparkmall.common.bean.AdsLogInfo
import com.mxl.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream

//需求6：每天各地区各城市各广告点击量实时统计
object AreaCityAdClickCountPerDayApp {

  def staticClick(notOnBlackListDstream: DStream[AdsLogInfo]): DStream[(String, Int)] = {
    val key = "day:area:city:ad:click" //hash类型

    val fieldToOneDs: DStream[(String, Int)] = notOnBlackListDstream.map({
      case AdsLogInfo(ts, area, city, _, ad) =>
        val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
        (s"$dayString:$area:$city:$ad", 1)
    })

    val resultDstream: DStream[(String, Int)] = fieldToOneDs.updateStateByKey {
      case (seq: Seq[Int], opt: Option[Int]) =>
        Some(seq.sum + opt.getOrElse(0))
    }

    //写入redis
    resultDstream.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        val jedis = RedisUtil.getJedis
        iterator.toList.foreach {
          case (field, count) =>
            jedis.hset(key, field, count.toString)
        }
        jedis.close()
      })
    })

    resultDstream
  }
}
