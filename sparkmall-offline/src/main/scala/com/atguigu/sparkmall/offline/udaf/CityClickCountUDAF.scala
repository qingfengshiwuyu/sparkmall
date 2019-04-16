package com.atguigu.sparkmall.offline.udaf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CityClickCountUDAF extends UserDefinedAggregateFunction{
  //输入的数据类型  string
  override def inputSchema: StructType = {
    StructType(StructField("city_name",StringType)::Nil)
  }

  //里面存储的数据类型  Map   和   long
  override def bufferSchema: StructType = {
    StructType(StructField("citycount",MapType(StringType,LongType))::StructField("totalcount",LongType)::Nil)
  }

  //输出的类型
  override def dataType: DataType = StringType
  //类型检查，是否有相同输入时有相同的输出，一般情况下都是它
  override def deterministic: Boolean = true

  //初始化工作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }


  //每个分区内的合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val city = input.getString(0)
    buffer(0) = map + (city->(map.getOrElse(city,0L) + 1L))
    buffer(1) = buffer.getLong(1) + 1L
  }
  //总体的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
    val map2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)

    buffer1(0) = map1 ++ map2.map{
      case (k,v) => (k->(map1.getOrElse(k,0L) + v))
    }

    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  //输出
  override def evaluate(buffer: Row): Any = {
    val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val totalCount: Long = buffer.getLong(1)
    val cityRemarks: List[CityRemark] = map.toList.sortWith(_._2 > _._2).take(2).map {
      case (cityName, count) => {
        CityRemark(cityName, count.toDouble / totalCount)
      }
    }
    val allRemark: List[CityRemark] = cityRemarks:+ CityRemark("其他",cityRemarks.foldLeft(1D)(_ - _.cityRatio))

    //这里会调用它的tostring方法
    allRemark.mkString(",")
  }
}

case class CityRemark(cityName:String,cityRatio:Double){
  private val formatter = new DecimalFormat(".00%")

  override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
}
