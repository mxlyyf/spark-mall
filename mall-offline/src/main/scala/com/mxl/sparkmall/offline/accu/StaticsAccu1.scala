package com.mxl.sparkmall.offline.accu

import com.mxl.sparkmall.common._
import com.mxl.sparkmall.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

class StaticsAccu1 extends AccumulatorV2[RDD[UserVisitAction], Map[String, (Long, Long, Long)]] {
  override def isZero: Boolean = ???

  override def copy(): AccumulatorV2[RDD[UserVisitAction], Map[String, (Long, Long, Long)]] = ???

  override def reset(): Unit = ???

  override def add(v: RDD[UserVisitAction]): Unit = ???

  override def merge(other: AccumulatorV2[RDD[UserVisitAction], Map[String, (Long, Long, Long)]]): Unit = ???

  override def value: Map[String, (Long, Long, Long)] = ???
}
