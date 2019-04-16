package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.common.util.{ConfigurationUtil, JDBCUtil}
import com.atguigu.sparkmall.offline.udaf.CityClickCountUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaClickTop3App {
  /**
    * 各区域热门商品top3  华北	商品A	100000	北京21.2%，天津13.2%，其他65.6%
    * @param spark
    */
  def statAreaClickTop3Product(spark: SparkSession) = {
    //1、取得行为表与城市表join，得到有城市名称的商品表
    //注册udaf函数
    spark.udf.register("city_remark",new CityClickCountUDAF)
    spark.sql("use sparkmall")
    spark.sql(
      """
        |select
        |   c.*,
        |   v.click_product_id
        |from city_info c join user_visit_action v on c.city_id = v.city_id
        |where v.click_product_id > -1
      """.stripMargin).createOrReplaceTempView("t1")

    //2、按照地区和商品进行聚集，需要自定义UDAF函数，进行各城市的比例
    spark.sql(
      """
        |select
        |   t1.area,
        |   t1.click_product_id,
        |   count(*) click_count,
        |   city_remark(t1.city_name) remark
        |from t1 group by t1.area,t1.click_product_id
      """.stripMargin).createOrReplaceTempView("t2")


    //3、按点击次数排序
    spark.sql(
      """
        |select
        |   *,
        |   rank() over(partition by t2.area order by t2.click_count desc) rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    //4、取前三名

    val conf = ConfigurationUtil("config.properties")

    val props = new Properties()
    props.setProperty("user",conf.getString("jdbc.user"))
    props.setProperty("password",conf.getString("jdbc.password"))
    JDBCUtil.executeUpdate("truncate area_click_top10",null)
    spark.sql(
      """
        |select
        |   t3.area,
        |   p.product_name,
        |   t3.click_count,
        |   t3.remark,
        |   t3.rank
        |from t3 join product_info p on t3.click_product_id= p.product_id
        |where t3.rank <= 3
      """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(conf.getString("jdbc.url"),"area_click_top10",props)//写入到mysql中

  }

}
