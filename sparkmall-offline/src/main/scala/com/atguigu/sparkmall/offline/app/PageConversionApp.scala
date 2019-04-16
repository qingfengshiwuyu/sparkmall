package com.atguigu.sparkmall.offline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConversionApp {

  /**
    * /计算页面跳转率
    *
    * @param spark
    * @param userVisitActionRDD
    * @param targetPageFlow 要统计的页面 1,2,3,4,5,6
    * @param taskId
    */
  def calcPageConversion(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], targetPageFlow: String, taskId: String) = {
    //处理页面
    val pageFlowArr: Array[String] = targetPageFlow.split(",")
    //从这个页面跳转
    val prePageFlowArr: Array[String] = pageFlowArr.slice(0, pageFlowArr.length - 1)
    //到这个页面
    val postPageFlowArr: Array[String] = pageFlowArr.slice(1, pageFlowArr.length)

    //明确需要统计那些页面的跳转率 (1-2) (2-3) (3-4) (4-5) (5-6)
    val targetJumpPages: Array[String] = prePageFlowArr.zip(postPageFlowArr).map(t => t._1 + "-" + t._2)


    //过滤用户行为中的页面，然后统计每个页面的个数(1 ,10)
    val targetPageCount: collection.Map[Long, Long] = userVisitActionRDD.filter(uva => pageFlowArr.contains(uva.page_id.toString))
      .map(uva => (uva.page_id, 1)).countByKey()



    //按每个session聚合，按它的点击时间排序，取出它这次页面跳转页面组合来，过滤出和上面需要统计的一样的页面
    val pageJumpRDD: RDD[String] = userVisitActionRDD.groupBy(_.session_id).flatMap {
      case (sid, action) => {
        //按时间排序
        val visitActions: List[UserVisitAction] = action.toList.sortBy(_.action_time)
        val pre: List[UserVisitAction] = visitActions.slice(0, visitActions.length - 1)
        val post: List[UserVisitAction] = visitActions.slice(1, visitActions.length)
        //取出它的页面
        pre.zip(post).map(t => t._1.page_id + "-" + t._2.page_id).filter(targetJumpPages.contains(_))

      }
    }
    //计算出页面跳转的个数（1-2,10）
    val pageJumpCount: Array[(String, Int)] = pageJumpRDD.map((_,1)).reduceByKey(_+_).collect()

    //计算跳转率
    val formatter = new DecimalFormat(".00%")

    val conversionRate: Array[(String, String)] = pageJumpCount.map {
      case (p2p, jumpCount) => {
        //拿出每一个跳转率的第一个数数量
        val visitCount: Long = targetPageCount.getOrElse(p2p.split("-").head.toLong, 0L)
        val rate: String = formatter.format(jumpCount.toDouble / visitCount)
        (p2p, rate)
      }
    }



    //存入到mysql中
    // 7. 存储到数据库
    val result: Array[Array[Any]] = conversionRate.map {
      case (p2p, conversionRate) => Array[Any](taskId, p2p, conversionRate)
    }
    JDBCUtil.executeUpdate("truncate page_conversion_rate", null)
    JDBCUtil.executeBatchUpdate("insert into page_conversion_rate values(?, ?, ?)", result)

  }

}
