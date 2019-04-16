package com.atguigu.sparkmall.common.util


import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  private val config = ConfigurationUtil("config.properties")
  private val host: String = config.getString("redis.host")
  private val port = config.getInt("redis.port")


  val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
  jedisPoolConfig.setMaxTotal(100) //最大连接数
  jedisPoolConfig.setMaxIdle(20) //最大空闲
  jedisPoolConfig.setMinIdle(20) //最小空闲
  jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
  jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
  jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

  private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, host, port)

  def getJedisClient:Jedis={
    jedisPool.getResource
  }

}