package com.mxl.sparkmall.common.util

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
    * 得到一个随机时间
    * @return
    */
  def getRandomDate = {
    // 上次 action 的时间
    var lastDateTIme: Long = 0
    System.currentTimeMillis()
    // 每次最大的步长时间r
    var maxStepTime: Long = 0
    // 这次操作的相比上次的步长
    val timeStep: Long = randomLong(0, maxStepTime)
    lastDateTIme += timeStep
    new Date(lastDateTIme)
  }

}
