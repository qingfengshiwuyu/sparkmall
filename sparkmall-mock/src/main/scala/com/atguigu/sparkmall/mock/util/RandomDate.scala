package com.atguigu.sparkmall.mock.util

import java.text.SimpleDateFormat
import java.util.Date


/**
  * 设置时间的类，这里是为了，让我们产生时间时能够使得第二次总比第一次大
  */
object RandomDate {

  def apply(startTime:Date,endTime:Date,step:Int)={

    val randomDate = new RandomDate
    val avgStepTime = (endTime.getTime - startTime.getTime) / step

    randomDate.maxStepTime = 4 * avgStepTime
    randomDate.lastDateTime= startTime.getTime
    randomDate
  }

  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val startTime: Date = format.parse("2019-03-20")
    val toTime: Date = format.parse("2019-03-24")

    val date = RandomDate(startTime,toTime,10000)
    println(date.getRandomDate)
    println(date.getRandomDate)
    println(date.getRandomDate)
    println(date.getRandomDate)
    println(date.getRandomDate)
  }
}

class RandomDate{
  var lastDateTime : Long = _
  var maxStepTime : Long = _


  def getRandomDate = {
    val timeStep: Long = RandomNumUtil.randomLong(0,maxStepTime)
    lastDateTime += timeStep
    new Date(lastDateTime)
  }
}
