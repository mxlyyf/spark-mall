package com.mxl.sparkmall.realtime

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mxl.sparkmall.common.bean.StartupLog
import com.mxl.sparkmall.common.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//当日活跃用户及分时趋势图，昨日对比图
object DailyActiveUserApp {
  def main(args: Array[String]): Unit = {
    val topic = "sparkmall_start_log_topic"
    val conf: SparkConf = new SparkConf().setAppName("DailyActiveUserApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    //设置checkpoint
    sc.setCheckpointDir("hdfs://192.168.213.101:9000/spark-checkpoint")

    //从kafka获取对应topic的Dstream
    val consumerRecordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, topic)

    //封装到样例类StartupLog
    val startupLogDStream: DStream[StartupLog] = consumerRecordDStream.map {
      case record: ConsumerRecord[String, String] =>
        JSON.parseObject(record.value, classOf[StartupLog])
    }

    //Kafka数据过滤去重
    val dateUidDstream: DStream[(String, String)] = startupLogDStream.map {
      case log: StartupLog => ((log.logDate, log.uid), log)
    }.groupByKey().map {
      case ((date, uid), it) => (date, uid)
    }

    // ->redis去重
    val resultDstream: DStream[(String, String)] = dateUidDstream.transform(rdd => {
      val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val jedis = RedisUtil.getJedis
      val uidSet: util.Set[String] = jedis.smembers("DAU:" + currentDate)
      jedis.close
      //uids需要广播到executor节点
      val uidsBC: Broadcast[util.Set[String]] = sc.broadcast(uidSet)
      rdd.filter({
        case (date, uid) => {
          !uidsBC.value.contains(uid)
        }
      })
    })
    resultDstream.print

    //保存到redis
    resultDstream.foreachRDD(rdd => {
      rdd.foreachPartition {
        case it =>
          val jedis = RedisUtil.getJedis
          it.toList.map {
            case (date, uid) => {
              jedis.sadd("DAU:" + date, uid)
            }
          }
          jedis.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
