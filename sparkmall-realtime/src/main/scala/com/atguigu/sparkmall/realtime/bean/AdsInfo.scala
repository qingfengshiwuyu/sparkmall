package com.atguigu.sparkmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
  * timestamp   area    city    userid  adid
  * 某个时间点 某个地区  某个城市   某个用户  某个广告
  */
case class AdsInfo(ts: Long, area: String, city: String, userId: String, adsId: String) {
  val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
  override def toString: String = s"$dayString:$area:$city:$adsId"
}