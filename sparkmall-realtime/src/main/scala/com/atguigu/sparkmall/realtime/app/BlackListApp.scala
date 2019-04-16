package com.atguigu.sparkmall.realtime.app

import java.util

import com.atguigu.sparkmall.realtime.bean.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListApp {
  val redisIP = "hadoop102"
  val redisPort = 6279
  //统计一个人一天的点击次数，用hash来表示
  // key  user:day:adsclick
  // value  $user:$day:$adsclick -> count
  val countKey = "user:day:adsclick"
  val blackLisKey = "blackList"

  /**
    * 将那些一天中广告次数大于100的加入到黑名单中
    * @param asdInfoDStream
    */
  def checkUserToBlackList(asdInfoDStream:DStream[AdsInfo]) ={

    //遍历每一个rdd
    asdInfoDStream.foreachRDD(rdd =>{
      //每一个rdd遍历每一个分区
      rdd.foreachPartition(adsInfoIt =>{
        //创建Redis的客户端
        val jedis = new Jedis(redisIP,redisPort)
        //每一个分区中遍历每一天数据
        adsInfoIt.foreach(adsInfo =>{
          val countField = s"${adsInfo.userId}:${adsInfo.dayString}:${adsInfo.adsId}"

          jedis.hincrBy(countKey,countField,1)//每来一条数据，每个人每天每条广告就会+1
          //如果每个人每天每条广告的点击次数大于100，就加入到黑名单中
          if(jedis.hget(countKey,countField).toLong >= 100){
            jedis.sadd(blackLisKey,adsInfo.userId)
          }
        })
        jedis.close()
      })
    })

  }

  //把那些黑名单中的人员过滤掉，放回值是不包含黑明单中的DStream
  def checkUserFromBlackList(asdInfoDStream:DStream[AdsInfo],sc:SparkContext)={


    asdInfoDStream.transform(rdd =>{
      val jedis = new Jedis(redisIP,redisPort)
      //黑名单中的人名
      val blackList: util.Set[String] = jedis.smembers(blackLisKey)
      //广播变量
      val blackListDB: Broadcast[util.Set[String]] = sc.broadcast(blackList)

      jedis.close()

      rdd.filter(adsInfo => {
        //不包含黑名单的人员
        !blackListDB.value.contains(adsInfo.userId)
      })
    })
  }

}
