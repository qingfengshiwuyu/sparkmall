package com.atguigu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.util.RedisUtil
import com.atguigu.sparkmall.realtime.bean.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsPerDay {
  /**
    * 每天每区域每城市每个广告的实时统计
    * @param filterAdsInfoDStream
    * @param sc
    * @return
    */
  def statAreaCityAdsPerDay(filterAdsInfoDStream:DStream[AdsInfo],sc:SparkContext) ={
    //设置checkPoint目录
    sc.setCheckpointDir("./checkpoint")
    //按照每天每个区域每个城市每个广告做统计
    val countAds: DStream[(String, Long)] = filterAdsInfoDStream.map(adsInfo => {
      (s"${adsInfo.dayString}:${adsInfo.area}:${adsInfo.city}:${adsInfo.adsId}", 1L)
    }).reduceByKey(_ + _)

    //做有状态装换
    val resultDStream: DStream[(String, Long)] = countAds.updateStateByKey((seq: Seq[Long], opt: Option[Long]) => {
      Some(seq.sum + opt.getOrElse(0L))
    })




    //写入到Redis中
    resultDStream.foreachRDD(rdd => {
      val jedis: Jedis = RedisUtil.getJedisClient
      val totalCountArr: Array[(String, Long)] = rdd.collect()

      totalCountArr.foreach{
        case (field,count) => jedis.hset("day:area:city:adsCount",field,count.toString)
      }
      jedis.close()
    })

    resultDStream
  }

}
