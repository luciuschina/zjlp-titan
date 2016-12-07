package com.zjlp.face.spark.base

import com.zjlp.face.spark.utils.SparkUtils
import com.zjlp.face.titan.TitanInit
import com.zjlp.face.titan.impl.TitanDAOImpl
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

case class UserVertexId(val userId: String, val vertexId: String)
case class FriendShip(val userId:String, val friendUserId:String)
class DataMigration(val spark: SparkSession) extends scala.Serializable {
  private val titanDAO = new TitanDAOImpl()
  private def getIdMapFromES = {
    spark.sql(
      s"CREATE TEMPORARY TABLE userIdVIdInES " +
        s"USING org.elasticsearch.spark.sql " +
        s"OPTIONS (resource '${Props.get("titan-es-index")}/rel', es.read.metadata 'true')")
    spark.sql("SELECT _metadata._id as userIdInES, vertexId FROM userIdVIdInES")
      .createOrReplaceTempView("userIdVIdMap")
    spark.sql("cache table userIdVIdMap")
  }

  def addUsers() = {
    import spark.implicits._
    spark.sql("select distinct userId from relation")
      .map(r => r(0).toString).distinct().foreachPartition {
      userIdRDD =>
        val titanDao = new TitanDAOImpl()
        userIdRDD.foreach(userId => titanDao.addUser(userId, titanDao.getTitanGraph))
        titanDao.closeTitanGraph()
    }
  }

  def addRelations() = {
    import spark.implicits._
    getIdMapFromES
    spark.sql("select distinct r2.userId as userId1,r1.userId as userId2 from relation r1 inner join (select distinct loginAccount,userId from relation)r2 on r2.loginAccount = r1.username").createOrReplaceTempView("relByUserId")
    spark.sql("select userIdVId1,vertexId as userIdVId2 from  (select vertexId as userIdVId1,userId2 from relByUserId inner join userIdVIdMap on userIdInES = userId1) b inner join userIdVIdMap on userIdInES = userId2")
      .map(r => (r(0).toString, r(1).toString))
      .distinct()
      .foreachPartition {
        pairRDDs =>
          val titanDao: TitanDAOImpl = new TitanDAOImpl()
          pairRDDs.foreach {
            pairRDD =>
              titanDao.addRelationByVID(pairRDD._1, pairRDD._2, titanDao.getTitanGraph(0))
          }
          titanDao.getTitanGraph(0).tx().commit()
          titanDao.closeTitanGraph()
      }
    SparkUtils.dropTempTable(spark, "relByUserId")
  }

  /**
   * 数据同步，即对比titan与mysql中的数据是否一致，如果不一致就增量更新为与mysql中的一致
   **/
  private def relationsSyn(): Unit = {
    import spark.implicits._
    getIdMapFromES
    getVidRelationsFromTitan

    spark.sql("select userIdTitan, userIdInES as friendUserIdTitan " +
        "from (select userIdInES as userIdTitan ,friendVidTitan from VidRelTitan inner join userIdVIdMap " +
        "on vertexId = userVidTitan) a inner join userIdVIdMap on vertexId = friendVidTitan ")
        .createOrReplaceTempView("friendshipInTitan")

    spark.sql("select distinct r2.userId as userIdSql,r1.userId as friendUserIdSql from relation r1 inner join (select distinct loginAccount,userId from relation)r2 on r2.loginAccount = r1.username")
      .createOrReplaceTempView("friendshipInSql")

    val userInMysql = spark.sql("select distinct userId from relation")
      .map(r => r(0).toString)
    val userInTitan = spark.sql("select distinct userIdInES from userIdVIdMap").map(r => r(0).toString)
    userInMysql.except(userInTitan).collect().foreach {
      userId => titanDAO.addUser(userId, titanDAO.getTitanGraph);
    }

    spark.sql("select userIdSql,friendUserIdSql,userIdTitan,friendUserIdTitan from friendshipInTitan full outer  join friendshipInSql on userIdSql = userIdTitan and friendUserIdSql =  friendUserIdTitan where userIdSql is null or userIdTitan is null")
    .createOrReplaceTempView("resultTable")
    spark.sql("cache table resultTable")
    spark.sql("select userIdTitan as userId,friendUserIdTitan as friendUserId from resultTable where userIdTitan is not null")
      .as[FriendShip]
      .rdd.collect()
      .foreach(friendShip => titanDAO.deleteRelation(friendShip.userId, friendShip.friendUserId))

    spark.sql("select userIdSql as userId,friendUserIdSql as friendUserId from resultTable where userIdSql is not null")
      .as[FriendShip]
      .rdd.collect()
      .foreach(friendShip => titanDAO.addRelation(friendShip.userId, friendShip.friendUserId))
  }


  private def getVidRelationsFromTitan = {
    import spark.implicits._
    spark.sql("select distinct vertexId from userIdVIdMap ")
      .map(r => r(0).toString).mapPartitions {
      vidRDD =>
        val titan = new TitanDAOImpl()
        vidRDD.flatMap {
          vid => titan.getAllFriendVIds(vid).map(friendId => (vid, friendId.toString))
        }
    }.toDF("userVidTitan", "friendVidTitan").createOrReplaceTempView("VidRelTitan")
  }
}

object DataMigration extends scala.Serializable {
  def main(args: Array[String]) {
    val spark = Spark.session()
    val beginTime = System.currentTimeMillis()
    val dataMigration = new DataMigration(spark)

    val mysql = new JdbcDF(spark)
    mysql.cacheRelationFromMysql

    if (Props.get("clean-init-data").toBoolean) {
        val titanInit = new TitanInit()
        titanInit.run()
        dataMigration.addUsers()
        titanInit.userIdUnique(titanInit.getTitanGraph)
        dataMigration.addRelations()
    }

    if (Props.get("relation-syn").toBoolean) {
      dataMigration.relationsSyn()
    }

    SparkSession.clearActiveSession()
    println(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
    System.exit(0)
  }
}
