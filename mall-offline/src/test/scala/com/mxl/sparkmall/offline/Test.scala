package com.mxl.sparkmall.offline

import org.junit

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Test {
  @junit.Test
  def test03: Unit = {
    var map: Map[String, (Long, Long, Long)] = new HashMap[String, (Long, Long, Long)]

    map += "1" -> (1, 1, 1)
    map += "2" -> (2, 2, 2)

    map += "1" -> (2, 3, 1)
    println(map.size)
    map.foreach(println)
  }

  @junit.Test
  def test04: Unit = {
    var list: List[String] = List("a", "b", "c", "d")

    val ints1: List[Int] = 50 :: 40 :: 30 :: 20 :: 10 :: Nil

    val ints2: List[Int] = ints1 :+ 11 :+ 12

    val words: List[String] = "alice" :: "eillen" :: "jakck" :: Nil

    println(words.map(_.toUpperCase))
    println(words.flatMap(_.toUpperCase))

    println(list.contains("b"))
    println(ints1)
    println(ints2.filter(x => x % 2 == 0))
  }

  @junit.Test
  def test07 = {
    var list1 = List(10, 1, 3, 5, 8, 9, 22, 89)
    // 需求:计算1000减去集合中的所有的元素之后的差
    val result: Int = list1.fold(1000)((A1, A2) => A1 - A2)
    println(result)

    println(list1.scan(100)((x, y) => x - y))

  }

  @junit.Test
  def test05 = {
    val list1 = List(10, 1, 1, 5, 8, 9, 22, 89)
    val list2 = List("abc", "dds", "bed", "dae", "qwe", "wer", "ipo", "yui")
    val list3: List[(Int, String)] = list1.zip(list2)

    println(list1.sortBy(x => -x))
    println(list2.sortBy(x => x)(Ordering.String.reverse))
    println(list3.sortBy({
      case (x, y) => (x, y)
    })(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse)))
  }

  @junit.Test
  def test06: Unit = {
    var list: List[(String, String)] = List[(String, String)]("hunan" -> "chenzhou", "hunan" -> "hengyang", "hunan" -> "yueyang", "hunan" -> "changsha",
      "hubei" -> "wuhan", "hubei" -> "ezhou", "hubei" -> "huanggang", "hubei" -> "huangshi")
    list :+= ("zhejiang" -> "hangzhou")
    list :+= ("zhejiang" -> "suzhou")
    println(list.size)

    val stringToList: Map[String, List[(String, String)]] = list.groupBy({
      case (x, y) => x //按照x分组
    })
    println(stringToList)

    println(list.groupBy(_._1).mapValues(_.map(_._2)))
  }

  @junit.Test
  def test01: Unit = {
    val chars = "AAAAAAAAAAAAAAA BBBBBBBBBBBBBBBBBBBBB CCCCCCCCCCCCCCCCCCCCCC DDDDDDDDDDDDDDDDDDDDDDD"
    //val map = Map[Char,Int]()
    val list: ArrayBuffer[Char] = new ArrayBuffer[Char]()

    val chars2: ArrayBuffer[Char] = chars.foldLeft(list) {
      (list, c) =>
        list.append(c)
        list
    }
    println(chars2)

    var map: mutable.Map[Char, Int] = mutable.HashMap[Char, Int]()
    chars2.foldLeft(map)({
      (map, c) =>
        map += c -> (map.getOrElse(c, 0) + 1)
    })
    println(map)

  }


}

