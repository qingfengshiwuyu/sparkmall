package com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.acc.MapAccumulator
import com.atguigu.sparkmall.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


/**
  * 将从hive中读出来数据参数的rdd进行每一行读取，
  * 因为不同的事件中有不同的品类
  * 我们这里是定义一个累加器去相加，每一个事件的次数
  *
  * （（品类，事件），次数）
  */
object CategoryTop10App {
  /**
    * 统计热门品类top10
    * @param spark
    * @param userVisitActionRDD
    * @param taskId
    * @return
    */
  def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String):List[CategoryCountInfo]= {
    val acc = new MapAccumulator
    //sc去注册累加器
    spark.sparkContext.register(acc,"CategoryActionACC")
    //对每一条记录进行处理，利用累加器进行相加
    userVisitActionRDD.foreach{
      visitAction =>{
        if (visitAction.click_category_id != -1){
          acc.add((visitAction.click_category_id.toString,"click"))
        } else  if(visitAction.order_category_ids != null){
          visitAction.order_category_ids.split(",").foreach{
            oid =>acc.add((oid,"order"))
          }
        } else if(visitAction.pay_category_ids != null){
          visitAction.pay_category_ids.split(",").foreach{
            pid => acc.add((pid,"pay"))
          }
        }
      }
    }
    //取出累加器中的值map(("商品1","click") -> 1000,("商品1","order") -> 500,("商品2","click") -> 800,)
    //按照每一个商品进行聚合（“商品1”，map（("商品1","click") -> 1000,("商品1","order") -> 500））
    val actionCountByCaterotyIdMap: Map[String, mutable.Map[(String, String), Long]] = acc.value.groupBy(_._1._1)

    val categoryCountInfoList: List[CategoryCountInfo] = actionCountByCaterotyIdMap.map {
      case (cid, actionMap) => CategoryCountInfo(
        taskId,
        cid,
        actionMap.getOrElse((cid, "click"), 0l),
        actionMap.getOrElse((cid, "order"), 0l),
        actionMap.getOrElse((cid, "pay"), 0l)
      )
    }.toList
    //按照三个值来降序排序
    val sortedCategoryInfoList: List[CategoryCountInfo] = categoryCountInfoList.sortBy(info => (info.clickCount, info.orderCount, info.payCount))(Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse))

    val top10: List[CategoryCountInfo] = sortedCategoryInfoList.take(10)

    //存入数据库
    // 7. 插入数据库
    val argsList:List[Array[Any]] = top10.map(info => Array(info.taskId, info.categoryId, info.clickCount, info.orderCount, info.payCount))
    JDBCUtil.executeUpdate("truncate category_top10",null)
    JDBCUtil.executeBatchUpdate("insert into category_top10 values(?, ?, ?, ?, ?)", argsList)
    //放回top10，用来之后用
    top10

  }

  /**
    * 统计top10 类别中的，每个品类的top10活跃sessionid
    * @param spark
    * @param categoryTop10
    * @param userVisitActionRDD
    * @param taskId
    * @return
    */
  def statCategoryTop10Session(spark: SparkSession, categoryTop10: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {
    //取出top10 类别中的类别即可
    val categoryIdTop10: List[String] = categoryTop10.map(_.categoryId)
    //将这个类别广播出去
    val categoryIdTop10DB: Broadcast[List[String]] = spark.sparkContext.broadcast(categoryIdTop10)

    //过滤取出用户浏览记录中有 top10的记录
    val filteredActionRDD : RDD[UserVisitAction] = userVisitActionRDD.filter(info => categoryIdTop10DB.value.contains(info.click_product_id + ""))
    //转换为 ((商品，session)，1) 然后根据key进行汇总，得到((商品，session)，count)
    val categorySessionCountRDD: RDD[((Long, String), Int)] = filteredActionRDD.map(info => ((info.click_product_id, info.session_id), 1))
      .reduceByKey(_ + _)
    //将上面的结果转换为(商品，(session，count))，然后按商品进行聚合，之后用来排序
    val categorySessionGrouped: RDD[(Long, Iterable[(String, Int)])] = categorySessionCountRDD.map {
      case ((cid, sid), count) => (cid, (sid, count))
    }.groupByKey()
    //按照点击率排，取前10
    val sortedCategorySession: RDD[CategorySession] = categorySessionGrouped.flatMap {
      case (cid, it) => {
        it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).map {
          item => CategorySession(taskId, cid.toString, item._1, item._2)
        }
      }
    }


    //存入到mysql中
    val categorySessionArr: Array[Array[Any]] = sortedCategorySession.collect.map(item => Array(item.taskId,item.categoryId,item.sessionId,item.clickCount))

    JDBCUtil.executeUpdate("truncate category_top10_session_count",null)
    JDBCUtil.executeBatchUpdate("insert into category_top10_session_count values(?,?,?,?)",categorySessionArr)
  }
}
