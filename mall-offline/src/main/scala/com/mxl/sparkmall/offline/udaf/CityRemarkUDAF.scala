package com.mxl.sparkmall.offline.udaf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CityRemarkUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = new StructType().add("cityName", StringType)

  //缓冲区的数据类型  Map["北京"->100,""天津->200] 、 Long 总的点击量
  //各个区域的各个城市的商品点击数
  override def bufferSchema: StructType = new StructType().add("city_click_map", MapType(StringType, LongType))
    .add("area_total_click", LongType)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = false

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) { //城市名不为空
      val city_name = input.getString(0)
      val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
      buffer(0) = map + (city_name -> (map.getOrElse(city_name, 0L) + 1L))
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0)) {
      val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
      val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)

      buffer1(0) = map1.foldLeft(map2){ // 把map1中的减值对的值和map2d值合并, 最后缓存到buffer1中
        case (map, (city, count)) => map + (city -> (map.getOrElse(city, 0L) + count))
      }

      // 总的点击量合并
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

    }
  }

  // 最终的返回值
  override def evaluate(buffer: Row): Any = {
    //"北京21.2%，天津13.2%，其他65.6%"
    val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getAs[Long](1)

    var cityRate: List[CityRemark] = map.toList.sortBy(-_._2).take(2).map {
      case (cityName, count) => CityRemark(cityName, count.toDouble / totalCount)
    }
    cityRate :+= CityRemark("其他", cityRate.foldLeft(1d)(_ - _.rate))
    cityRate.mkString(", ")
  }
}

case class CityRemark(cityName: String, rate: Double) {
  val f = new DecimalFormat("0.00%")

  override def toString: String = s"$cityName:${f.format(rate)}"
}
