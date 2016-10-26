package com.zjlp.face.spark.base

import com.zjlp.face.spark.utils.SparkUtils

/**
 * Created by root on 10/24/16.
 */
class MySQL {
  def cacheRelationFromMysql = {
    val sqlContext = MySQLContext.instance()
    SparkUtils.dropTempTables(sqlContext, "relInES", "relation")
    sqlContext.read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> s"(select rosterId,username,loginAccount from of_roster where username != loginAccount) tb",
      "driver" -> Props.get("jdbc_driver"),
      "partitionColumn" -> "rosterId",
      "lowerBound" -> "1",
      "upperBound" -> getRosterUpperBound(),
      "numPartitions" -> Props.get("mysql_table_partition")
    )).load()
      .registerTempTable("relation")
    MySQLContext.instance().sql(s"cache table relation")
  }

  private def getRosterUpperBound(): String = {

    return MySQLContext.instance().read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> s"(select max(rosterId) from of_roster ) tb",
      "driver" -> Props.get("jdbc_driver")
    )).load().map(a => a(0).toString.toLong).max().toString

  }
}
