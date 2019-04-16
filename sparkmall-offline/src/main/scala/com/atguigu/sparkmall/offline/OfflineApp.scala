package com.atguigu.sparkmall.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.offline.app.{AreaClickTop3App, CategoryTop10App, PageConversionApp}
import com.atguigu.sparkmall.offline.bean.{CategoryCountInfo, Condition}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * top10热门的类别入口，，热门这里定义为点击，下单，支付人数的排行
  */
object OfflineApp {



  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive.warehouse")
      .getOrCreate()

    //定义一个taskID用来表示那一批的
    val taskId: String = UUID.randomUUID().toString

    //定义一个方法，用来过滤条件的读取数据
    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(spark,readConditions)

    println("任务1：开始")
    val categoryTop10: List[CategoryCountInfo] = CategoryTop10App.statCategoryTop10(spark,userVisitActionRDD,taskId)
    println("任务1：结束")

    println("任务2：开始")
    CategoryTop10App.statCategoryTop10Session(spark,categoryTop10,userVisitActionRDD,taskId)
    println("任务2：结束")

    println("任务3：开始")
    PageConversionApp.calcPageConversion(spark,userVisitActionRDD,readConditions.targetPageFlow,taskId)
    println("任务3：结束")

    println("任务4：开始")
    AreaClickTop3App.statAreaClickTop3Product(spark)
    println("任务4：结束")



  }

  def readUserVisitActionRDD(spark: SparkSession, condition: Condition) = {
    var sql = s"select v.* from user_visit_action v join user_info u on v.user_id = u.user_id where 1=1"

    //过滤添加，拼接SQL
    if(isNotEmpty(condition.startDate)){
      sql += s" and v.date >= '${condition.startDate}'"
    }
    if(isNotEmpty(condition.endDate)){
      sql += s" and v.date <= '${condition.endDate}'"
    }
    if(condition.startAge != 0){
      sql += s" and u.age >= '${condition.startAge}'"
    }
    if(condition.endAge != 0){
      sql += s" and u.age >= '${condition.endAge}'"
    }

    import spark.implicits._

    spark.sql("use sparkmall")
    spark.sql(sql).as[UserVisitAction].rdd
  }


  def readConditions:Condition ={
    //读取配置文件
    val configuration = ConfigurationUtil("conditions.properties")
    //将配置配置文件中年的json字符串度出来
    val conditionString: String = configuration.getString("condition.params.json")
    //用json来解析成对象
    JSON.parseObject(conditionString,classOf[Condition])
  }



}
