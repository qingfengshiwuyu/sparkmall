package com.atguigu.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 进来的数据是（品类，事件）
  * 累加之后的数据时map（（品类，事件）-> 100 ，（品类，事件）-> 200）
  * 如map（（“商品1”，“click”）-> 100 ，（“商品1”，“pay”）-> 50）
  */
class MapAccumulator extends AccumulatorV2[(String,String),mutable.Map[(String,String),Long]]{
  private val map: mutable.Map[(String, String), Long] = mutable.Map[(String,String),Long]()
  //判断是否为空
  override def isZero: Boolean = map.isEmpty
  //拷贝数据
  override def copy(): AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] = {
    val newACC: MapAccumulator = new MapAccumulator
    map.synchronized{
      newACC.map ++=map
    }
    newACC
  }
  //清空
  override def reset(): Unit = map.clear()
  //添加操作
  override def add(v: (String, String)): Unit = {
    //如果刚开始没有，则赋值为0，有则加1
    map(v) = map.getOrElseUpdate(v,0l) + 1l
  }
//合并
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[(String, String), Long]]): Unit = {
    //取出其他要合并的map来
    other.value.map{
      case (k,c) =>map.put(k,map.getOrElse(k,0l) + c)
    }

  }

  //返回数据
  override def value: mutable.Map[(String, String), Long] = map
}
