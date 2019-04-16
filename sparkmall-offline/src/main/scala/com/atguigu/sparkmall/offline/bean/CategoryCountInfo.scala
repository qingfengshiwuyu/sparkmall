package com.atguigu.sparkmall.offline.bean

//这个类用来封装，商品的各个类别的点击数量
case class CategoryCountInfo(taskId: String,
                             categoryId: String,
                             clickCount: Long,
                             orderCount: Long,
                             payCount: Long)