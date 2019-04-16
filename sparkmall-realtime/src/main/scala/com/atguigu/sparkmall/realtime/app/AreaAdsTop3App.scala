package com.atguigu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

object AreaAdsTop3App {
  /**
    * 每天每地区热门广告 Top3
    * @param areaCityAdsCountDS
    */
  def statAreaAdsTop3(areaCityAdsCountDS: DStream[(String, Long)])={


    //得到DStream[(2019-03-22:华南：1,1000)]
    val dayAreaAdsCount: DStream[(String, Long)] = areaCityAdsCountDS.map {
      case (dayAreaCityAds, count) => {
        val split: Array[String] = dayAreaCityAds.split(":")
        (s"${split(0)}:${split(1)}:${split(3)}", count)
      }
    }.reduceByKey(_ + _)  //

    //按照时间聚合，每一个小的单元为(Area,(ads,count))
    val areaAdsGroupByDay: DStream[(String, Iterable[(String, (String, Long))])] = dayAreaAdsCount.map {
      case (dayAreaAds, count) => {
        val split: Array[String] = dayAreaAds.split(":")
        //(day,(area,(ads,count)))
        (split(0), (split(1), (split(2), count)))
      }
    }.groupByKey()

    //按照点击去前三个，并将后面的装换为json字符串
    val resultDS: DStream[(String, Map[String, String])] = areaAdsGroupByDay.map {
      case (day, it) => {
        //得到 Map[华南，Iterable[(华南,(广告1,1000))]]
        val tem1: Map[String, Iterable[(String, (String, Long))]] = it.groupBy(_._1)

        // Map[华南，Iterable[(广告1,1000)]]
        val tem2: Map[String, Iterable[(String, Long)]] = tem1.map {
          case (area, it) => {
            (area, it.map(_._2)) // (day, it[(aids, count)]
          }
        }

        //对tem2中的降序取前三
        val tem3: Map[String, String] = tem2.map {
          case (area, it) => {
            val list: List[(String, Long)] = it.toList.sortWith(_._2 > _._2).take(3)
            import org.json4s.JsonDSL._ // 加载的隐式转换  json4s  是面向 scala 的 json 转换
            val adsCountJson: String = JsonMethods.compact(JsonMethods.render(list))

            (area, adsCountJson)
          }
        }

        (day, tem3)
      }
    }


    //存入到Redis中
    resultDS.foreachRDD{
      rdd => {
        val jedis: Jedis = RedisUtil.getJedisClient

        val arr: Array[(String, Map[String, String])] = rdd.collect()
        // 导入隐式转换, 用于把 scala 的 map 隐式转换成 Java 的 map
        import scala.collection.JavaConversions._
        arr.foreach{
          case (day,map) =>{
            jedis.hmset(s"area:ads:top3:$day",map)
          }
        }
        jedis.close()
      }
    }


  }

}
