package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import com.atguigu.sparkmall.realtime.app.BlackListApp
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
  private val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp").setMaster("local[*]")
  private val sc = new SparkContext(sparkConf)

  private val ssc = new StreamingContext(sc,Seconds(1))

  //得到Dstream
  private val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc,"ads_log")

  //从Dstream 中拿出数据，封装到一个对象中
  private val adsInfoDStream: DStream[AdsInfo] = kafkaDstream.map {
    record =>
      val splits: Array[String] = record.value().split(",")
      AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
  }

  println("任务5 开始")
  //先判断是否你在黑名单，拿出那些不含黑名单中的，然后在判断是否大于100
  private val filteredDStream: DStream[AdsInfo] = BlackListApp.checkUserFromBlackList(adsInfoDStream,sc)
  filteredDStream.re
  BlackListApp.checkUserToBlackList(filteredDStream)

  ssc.start()
  ssc.awaitTermination()


}
