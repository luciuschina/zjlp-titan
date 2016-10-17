package com.zjlp.face.spark.base

import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.Logging

object Props extends Logging {
  private val prop = new Properties()

  /**
   * 在spark-submit中加入--driver-java-options -DPropPath=/home/hadoop/prop.properties的参数后，
   * 使用System.getProperty("PropPath")就能获取路径：/home/hadoop/prop.properties如果spark-submit中指定了
   * prop.properties文件的路径，那么使用prop.properties中的属性，否则使用该类中定义的属性
   */
  private def getPropertyFile: String = {
    if (externalPropertiesExist) {
      logInfo(s"配置文件：${System.getProperty("PropPath")}")
      System.getProperty("PropPath")
    } else {
      logInfo(s"配置文件：${getClass.getResource("/").getPath() + "config.properties"}")
      getClass().getResource("/").getPath() + "config.properties"
    }
  }

  /**
   * 判断项目打成jar包运行时，是否传入了日志文件
   * @return
   */
  private def externalPropertiesExist: Boolean = Option(System.getProperty("PropPath")).isDefined

  //装载配置文件
  prop.load(new FileInputStream(getPropertyFile))

  /**
   * 根据配置文件中的的属性名获取属性值
   */
  def get(propertyName: String): String = {
    val value = prop.getProperty(propertyName)
    if (value == null) {
      throw new Exception("配置文件中沒有这个属性:" + propertyName)
    }
    new String(value.getBytes("ISO-8859-1"), "utf-8")
  }

}