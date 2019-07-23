package com.mxl.sparkmall.common.bean

import java.text.SimpleDateFormat
import java.util.Date

case class AdsLogInfo(ts: Long,
                      area: String,
                      city: String,
                      userId: String,
                      adsId: String) {
  val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
  val hmString: String = new SimpleDateFormat("HH-MM").format(new Date(ts))

  override def toString: String = s"$dayString:$area:$city:$userId:$adsId"
}
