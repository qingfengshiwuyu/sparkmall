package com.atguigu.sparkmall.offline.bean


/**
  * 定义一个类来封装json字符串的对象
  * @param startDate
  * @param endDate
  * @param startAge
  * @param endAge
  * @param professionals
  * @param city
  * @param gender
  * @param keywords
  * @param categoryIds
  * @param targetPageFlow
  */
case class Condition(var startDate: String,
                     var endDate: String,
                     var startAge: Int,
                     var endAge: Int,
                     var professionals: String,
                     var city: String,
                     var gender: String,
                     var keywords: String,
                     var categoryIds: String,
                     var targetPageFlow: String)
