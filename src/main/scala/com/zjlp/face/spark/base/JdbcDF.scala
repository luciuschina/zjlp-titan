package com.zjlp.face.spark.base

import java.util.Properties

import org.apache.spark.sql.SparkSession

class JdbcDF(val spark: SparkSession) {
  val MYSQL_CONNECTION_URL = Props.get("mysql_connection_url") //"jdbc:mysql://192.168.175.12:3306/spark_search"
  val connectionProperties = new Properties()
  connectionProperties.put("user", Props.get("mysql_connection_user"))
  connectionProperties.put("password", Props.get("mysql_connection_password"))
  val tablePartition = Props.get("mysql_table_partition").toString.toInt

  def cacheRelationFromMysql = {
    spark.read.jdbc(MYSQL_CONNECTION_URL, " (select rosterID as rosterId,username,loginAccount,userID as userId from view_ofroster where sub=3 and userID is not null and username != loginAccount) as tb", "rosterId", 1, getMaxRosterId(), tablePartition , connectionProperties)
    .createOrReplaceTempView("relation")
  }

  private def getMaxRosterId(): Long = {
    import spark.implicits._
    return spark.read.jdbc(MYSQL_CONNECTION_URL, "(select max(rosterId) from view_ofroster) as max_roster_id", connectionProperties)
      .map(r => r(0).toString.toLong).collect()(0)
  }

}
