package com.mxl.sparkmall.realtime

import java.util.Set

import com.mxl.sparkmall.common._
import com.mxl.sparkmall.common.util.{MyKafkaUtil, RedisUtil}
import com.mxl.sparkmall.realtime.app.{AreaCityAdClickCountPerDayApp, AreaHotAdTop3PerDay, BlackListApp}
import com.mxl.sparkmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RealTimeApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    //设置checkpoint
    sc.setCheckpointDir("hdfs://192.168.213.101:9000/spark-checkpoint")

    //从kafaka得到dstream
    val consumerRecordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, TOPIC)

    //封装到样例类中
    val adsLogDstream: DStream[AdsLogInfo] = consumerRecordDStream.map(record => {
      val params: Array[String] = record.value().split(",")
      AdsLogInfo(params(0).toLong, params(1), params(2), params(3), params(4))
    })

    //过滤掉那些已经在黑名单的user
    val notOnBlackListDstream: DStream[AdsLogInfo] = adsLogDstream.transform(rdd => {
      //从redis查询
      val jedis = RedisUtil.getJedis
      val blackListSet: Set[String] = jedis.smembers("blacklist")
      jedis.close
      //把黑名单广播出去
      val blkListBC: Broadcast[Set[String]] = sc.broadcast(blackListSet)
      rdd.filter(adsLog => {
        !blkListBC.value.contains(adsLog.userId)
      })
    })

    //需求5：广告黑名单实时统计
    BlackListApp.PutUserToBlackList(notOnBlackListDstream)

    //需求6：每天各地区各城市各广告点击量实时统计
    val areaCityAdClickCountPerDayDstream: DStream[(String, Int)] = AreaCityAdClickCountPerDayApp.staticClick(notOnBlackListDstream)
    areaCityAdClickCountPerDayDstream.print

    //需求7：每天各地区各城市热门广告Top3
    AreaHotAdTop3PerDay.areaHotAdTop3PerDay(areaCityAdClickCountPerDayDstream)

    //需求8：各广告最近 1 小时内各分钟的点击量


    ssc.start
    ssc.awaitTermination
  }

}
