package com.mxl.sparkmall.offline.accu

import com.mxl.sparkmall.common.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.immutable.HashMap

class StaticsAccu1 extends AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] {
  //点击总数、下单总数、支付总数
  var map: Map[String, (Long, Long, Long)] = new HashMap[String, (Long, Long, Long)]

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] = {
    val acc = new StaticsAccu1
    acc.map ++= map
    acc
  }

  override def reset(): Unit = map = new HashMap[String, (Long, Long, Long)]

  override def add(v: UserVisitAction): Unit = {

    if (v.click_category_id != -1) { //点击数累加
      var cid = v.click_category_id.toString
      val tuple: (Long, Long, Long) = map.getOrElse(cid, (0, 0, 0))
      map += cid -> (tuple._1 + 1, tuple._2, tuple._3)
    } else if (v.order_category_ids != null) { //下单数累加
      v.order_category_ids.split(",").foreach(cid => {
        val tuple: (Long, Long, Long) = map.getOrElse(cid, (0, 0, 0))
        map += cid -> (tuple._1, tuple._2 + 1, tuple._3)
      })
    } else if (v.pay_category_ids != null) { //支付数累加
      v.pay_category_ids.split(",").foreach(cid => {
        val tuple: (Long, Long, Long) = map.getOrElse(cid, (0, 0, 0))
        map += cid -> (tuple._1, tuple._2, tuple._3 + 1)
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]]): Unit = {
    other match {
      case o: StaticsAccu1 =>
        other.value.foreach {
          case (cid, (count1, count2, count3)) =>
            val tuple: (Long, Long, Long) = this.map.getOrElse(cid, (0, 0, 0))
            this.map += cid -> (count1 + tuple._1, count2 + tuple._2, count3 + tuple._3)
        }
      case _ =>
        throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: Map[String, (Long, Long, Long)] = this.map
}
