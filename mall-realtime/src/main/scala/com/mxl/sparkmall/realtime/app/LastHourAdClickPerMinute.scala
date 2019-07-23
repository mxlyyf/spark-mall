package com.mxl.sparkmall.realtime.app

import com.mxl.sparkmall.common.bean.AdsLogInfo
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

object LastHourAdClickPerMinute {
  def statisticsLastHourAdClickPerMinute(notOnBlackListDstream: DStream[AdsLogInfo]): Unit = {
    val windowDstream: DStream[AdsLogInfo] = notOnBlackListDstream.window(Minutes(60), Seconds(6))

    val adsMinuteClickDstream: DStream[((String, String), Int)] = windowDstream.map(
      adsLog => ((adsLog.adsId, adsLog.hmString), 1))
      .reduceByKey(_ + _)

    
  }
}
