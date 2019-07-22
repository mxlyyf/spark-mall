package com.mxl.sparkmall.realtime.app

import org.apache.spark.streaming.dstream.DStream

//需求7：每天各地区各城市热门广告Top3
////areaCityAdClickCountPerDayDstream 来自需求6dstream
object AreaHotAdTop3PerDay {
  def areaHotAdTop3PerDay(areaCityAdClickCountPerDayDstream: DStream[(String, Int)]): Unit ={

  }
}
