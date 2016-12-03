package com.zjlp.face.spark.base

import com.zjlp.face.titan.TitanConPool
import com.zjlp.face.titan.impl.EsDAOImpl
import org.apache.spark.sql.SparkSession

case class IfCache(val userId: String, val isCached: Boolean)

class CacheHotUser(spark: SparkSession) extends scala.Serializable {
  private val friendNumberOfHotUser = Props.get("friend-num-of-hot-user").toInt
  val esDAO = new EsDAOImpl()

  def updateHotUsers = {
    import spark.implicits._
    val hotUserInMysql = spark.sql("select userId,count(1) as friendNum from relation group by userId ")
      .map(r => (r(0).toString, r(1).toString.toInt)).filter(_._2 >= friendNumberOfHotUser).map(a => a._1).persist()
    val hotUserInES = spark.sql("select userIdInES from hotUser where isCached = true").map(r => r(0).toString)
    esDAO.multiCreateIfCache(hotUserInMysql.except(hotUserInES).map(IfCache(_, true)).collectAsList())
    esDAO.multiCreateIfCache(hotUserInES.except(hotUserInMysql).map(IfCache(_, false)).collectAsList())
  }

  def cacheHotUserFromES = {
    spark.sql(
      s"CREATE TEMPORARY TABLE hotUserCacheInES " +
        s"USING org.elasticsearch.spark.sql " +
        s"OPTIONS (resource '${Props.get("titan-es-index")}/ifcache', es.read.metadata 'true')")
    spark.sql("SELECT _metadata._id as userIdInES, isCached FROM hotUserCacheInES")
      .createOrReplaceTempView("hotUser")
    spark.sql("cache table hotUser")
  }
}

object CacheHotUser extends scala.Serializable {
  def main(args: Array[String]) {
    val spark = Spark.session()
    val mysql = new JdbcDF(spark)
    mysql.cacheRelationFromMysql
    val chu = new CacheHotUser(spark)
    chu.cacheHotUserFromES
    chu.updateHotUsers
    if (Props.get("clean-titan-instances").toBoolean) new TitanConPool().killAllTitanInstances()
  }
}
