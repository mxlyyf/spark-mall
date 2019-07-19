package com.mxl.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.{Date, Random}

import scala.collection.mutable

object RandomUtil {
  val random = new Random()

  /**
    * 返回一个随机的整数 [from, to]
    *
    * @param from
    * @param to
    * @return
    */
  def randomInt(from: Int, to: Int): Int = {
    if (from > to) throw new IllegalArgumentException(s"from = $from 应该小于 to = $to")
    // [0, to - from)  + from [form, to -from + from ]
    random.nextInt(to - from + 1) + from
  }

  /**
    * 随机的Long  [from, to]
    *
    * @param from
    * @param to
    * @return
    */
  def randomLong(from: Long, to: Long): Long = {
    if (from > to) throw new IllegalArgumentException(s"from = $from 应该小于 to = $to")
    random.nextLong().abs % (to - from + 1) + from
  }

  /**
    * 生成一系列的随机值
    *
    * @param from
    * @param to
    * @param count
    * @param canReat 是否允许随机数重复
    */
  def randomMultiInt(from: Int, to: Int, count: Int, canReat: Boolean = true): List[Int] = {
    if (canReat) {
      (1 to count).map(_ => randomInt(from, to)).toList
    } else {
      val set: mutable.Set[Int] = mutable.Set[Int]()
      while (set.size < count) {
        set += randomInt(from, to)
      }
      set.toList
    }
  }

  /**
    * 得到指定日期区间内的一个随机时间
    *
    * @return
    */
  def getRandomDate(startDate: String, endDate: String) = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

    val startTime: Long = dateFormatter.parse(startDate).getTime
    val endTime: Long = dateFormatter.parse(endDate).getTime

    val randomTime: Long = startTime + ((endTime - startTime) * Math.random()).toLong

    new Date(randomTime)
  }

}
