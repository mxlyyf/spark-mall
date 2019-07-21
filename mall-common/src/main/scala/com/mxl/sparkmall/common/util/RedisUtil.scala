package com.mxl.sparkmall.common.util

import com.mxl.sparkmall.common._
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  private val poolConfig: JedisPoolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(100) //最大连接数
  poolConfig.setMaxIdle(20) //最大空闲
  poolConfig.setMinIdle(20) //最小空闲
  poolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
  poolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
  poolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
  private val jedisPool: JedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT)

  // 直接得到一个 Redis 的连接
  def getJedis: Jedis = {
    jedisPool.getResource
  }
}
