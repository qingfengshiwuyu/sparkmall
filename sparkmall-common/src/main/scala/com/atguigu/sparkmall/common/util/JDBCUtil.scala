package com.atguigu.sparkmall.common.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory

object JDBCUtil {


  //dataSource通过方法创建
  val dataSource = initConnection()


  def initConnection()={
    val properties = new Properties()

    val config = ConfigurationUtil("config.properties")
    properties.put("driverClassName","com.mysql.jdbc.Driver")
    properties.put("url",config.getString("jdbc.url"))
    properties.put("username",config.getString("jdbc.user"))
    properties.put("password",config.getString("jdbc.password"))
    properties.put("maxActive",config.getString("jdbc.maxActive"))
    //通过德鲁伊来创建DataSource
    DruidDataSourceFactory.createDataSource(properties)
  }

  /**
    * 执行单条语句
    * inset into table values (?,?,?,....)
    * @param sql
    * @param args  参数，
    */
  def executeUpdate(sql:String,args:Array[Any])={

    //创建连接
    val conn: Connection = dataSource.getConnection
    //取消自动提交
    conn.setAutoCommit(false)
    val ps: PreparedStatement = conn.prepareStatement(sql)

    //判断参数是否为空
    if(args != null && args.length > 0){
      (0 to args.length).foreach{
            //设置参数
        i => ps.setObject(i+1,args(i))
      }
    }

    ps.executeUpdate()
    conn.commit()
  }

  /**
    * 批量执行
    */

  def executeBatchUpdate(sql: String, argsList: Iterable[Array[Any]]) = {
    val conn = dataSource.getConnection
    conn.setAutoCommit(false)
    val ps = conn.prepareStatement(sql)
    argsList.foreach {
      case args: Array[Any] => {
        (0 until args.length).foreach {
          i => ps.setObject(i + 1, args(i))
        }
        ps.addBatch()
      }
    }
    ps.executeBatch()
    conn.commit()
  }

}
