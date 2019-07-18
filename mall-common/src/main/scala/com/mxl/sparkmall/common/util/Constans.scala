package com.mxl.sparkmall.common.util

object Constans {
  //mysql配置
  val JDBC_URL = "jdbc:mysql://192.168.213.101:3306/sparkmall?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true"
  val JDBC_USER = "root"
  val JDBC_PASSWORD = "123456"

  //Kafka broker配置
  val KAFKA_BROKER_LIST = "hadoop101:9092,hadoop102:9092,hadoop103:9092"

  //Redis配置
  val REDIS_HOST = "192.168.213.101"
  val REDIS_PORT = "6379"

  //# hive数据库名字(选配)
  val HIVE_DATABASE = "sparkmall"
}
