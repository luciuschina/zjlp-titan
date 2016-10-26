package com.zjlp.face.spark.base

import com.zjlp.face.spark.utils.SparkUtils
import org.apache.spark.Logging
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata._

class CacheHotUser extends Logging with scala.Serializable {
  private val friendNumberOfHotUser = Props.get("friend-num-of-hot-user").toInt

  def updateHotUsers = {
    val sqlContext = MySQLContext.instance()
    val hotUserInMysql = sqlContext.sql("select username,count(1) from relation group by username ")
      .map(r => (r(0).toString, r(1).toString.toInt)).filter(_._2 >= friendNumberOfHotUser).map(a=> a._1).persist()
    val hotUserInES = sqlContext.sql("select usernameInES from hotUser where isCached = true").map(r => r(0).toString)
    val userCache = hotUserInMysql.subtract(hotUserInES).map(a => Tuple2(Map(ID -> a), Map("isCached" -> true)))
    EsSpark.saveToEsWithMeta(userCache, s"titan-es/ifcache")
    val userUncache = hotUserInES.subtract(hotUserInMysql).map(a => Tuple2(Map(ID -> a), Map("isCached" -> false)))
    EsSpark.saveToEsWithMeta(userUncache, s"titan-es/ifcache")
  }

  def cacheHotUserFromES = {
    val sqlContext = MySQLContext.instance()
    SparkUtils.dropTempTables(sqlContext, "relInES", "usernameVertexIdMap")
    sqlContext.sql(
      s"CREATE TEMPORARY TABLE hotUserCacheInES " +
        s"USING org.elasticsearch.spark.sql " +
        s"OPTIONS (resource '${Props.get("titan-es-index")}/ifcache', es.read.metadata 'true')")
    sqlContext.sql("SELECT _metadata._id as usernameInES, isCached FROM hotUserCacheInES")
      .registerTempTable("hotUser")
    sqlContext.sql("cache table hotUser")
  }
}

object CacheHotUser extends Logging with scala.Serializable {
  def main(args: Array[String]) {
    val beginTime = System.currentTimeMillis()
    val mysql = new MySQL()
    mysql.cacheRelationFromMysql
    val chu = new CacheHotUser()
    chu.cacheHotUserFromES
    chu.updateHotUsers
    logInfo(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
  }
}
