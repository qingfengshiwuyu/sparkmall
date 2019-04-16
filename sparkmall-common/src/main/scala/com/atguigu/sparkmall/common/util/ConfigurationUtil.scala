package com.atguigu.sparkmall.common.util


import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

import scala.collection.mutable

object ConfigurationUtil {
  private val configs: mutable.Map[String, FileBasedConfiguration] = mutable.Map[String,FileBasedConfiguration]()

  def apply(propertiesFileName:String)={
    configs.getOrElseUpdate(propertiesFileName,
      new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration]).configure(new Parameters().properties().setFileName(propertiesFileName)).getConfiguration
    )
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigurationUtil("config.properties")
    println(conf.getString("jdbc.user"))
  }

}
