package com.atguigu.sparkmall.mock.util

import scala.collection.mutable.ListBuffer

/**
  * 设置比重的类
  */
object RandomOptions {

  def apply[T](opts:(T,Int)*)={
    val randomOptions = new RandomOptions[T]()
    //总的个数等于传进来的数的总和，这里利用的左折叠
    randomOptions.totalWeight = (0 /:opts)(_ + _._2)
    opts.foreach{
      //你传进来的数字有多少，则在options集合添加多少个，这样，在集合中国随机去的时候，就能达到比重的效果
      case (value,weight) => randomOptions.options ++=(1 to weight).map(_ =>value)
    }
    randomOptions
  }
}

class RandomOptions[T]{

  var totalWeight:Int = _
  var options = ListBuffer[T]()

  /**
    * 获取随机的option的值
    * @return
    */
  def getRandomOption()={
    options(RandomNumUtil.randomInt(0,totalWeight-1))
  }
}
