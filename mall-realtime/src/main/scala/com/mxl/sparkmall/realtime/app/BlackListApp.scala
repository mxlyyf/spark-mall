package com.mxl.sparkmall.realtime.app

import java.{lang, util}

import com.mxl.sparkmall.common.util.RedisUtil
import com.mxl.sparkmall.realtime.AdsLogInfo
import org.apache.spark.streaming.dstream.DStream

object BlackListApp {
  //加入黑名单
  def PutUserToBlackList(notOnBlackListDstream: DStream[AdsLogInfo]) = {
    val key_blacklist = "blacklist"//set类型
    val key = "day:user:ad"        //hash类型

    notOnBlackListDstream.foreachRDD(rdd => {
      rdd.foreachPartition(adsLogIterator => {
        //
        val jedis = RedisUtil.getJedis

        adsLogIterator.foreach(adsLog => {
          val userId = adsLog.userId
          val field = s"${adsLog.dayString}:${adsLog.userId}:${adsLog.adsId}"
          //向redis的hashset的field的值增加1，返回的就是加1后的点击总数
          val clickCount: lang.Long = jedis.hincrBy(key, field, 1)
          if (clickCount > 1000) {
            //加入黑名单中
            jedis.sadd(key_blacklist, adsLog.userId)
          }
        })
        jedis.close()
      })
    })
  }
}