package com.zjlp.face.spark.base

import com.zjlp.face.spark.utils.SparkUtils
import com.zjlp.face.titan.{TitanConPool, TitanInit}
import com.zjlp.face.titan.impl.{EsDAOImpl, TitanDAOImpl}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

case class UserVertexId(val userId: String, val vertexId: String)
case class FriendShip(val userId:String, val friendUserId:String)
class DataMigration(val spark: SparkSession) extends scala.Serializable {
  private val esDAO = new EsDAOImpl()
  private val titanDAO = new TitanDAOImpl()
  private def getIdMapFromES = {
    //logInfo("从ES加载userId-vertexId索引数据")
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
   * 步骤：
   * 1、从ES中的titan-es索引中读取userId和VID的对应关系
   * 2、分片使用g.V(vid).out('knows').id 得到好友关系，可以得到 userVID - friendVID的对应关系
   * 3、结合步骤1和步骤2 从Titan中得到现有 userId - frienduserId 的对应关系
   * 4、从mysql中得到现有 userId - frienduserId 的对应关系
   * 5、比较有哪些新增的用户。往Titan中添加这些用户
   * 6、(步骤3的好友关系) substract (步骤4的好友关系) = 需要从Titan中删除的好友关系
   * 7、(步骤4的好友关系) substract (步骤3的好友关系) = 需要往Titan中添加的好友关系
   */
  private def relationsSyn(): Unit = {
    import spark.implicits._
    getIdMapFromES //1
    getVidRelationsFromTitan //2

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
    } //5

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

    val addUser = Props.get("add-user").toBoolean
    val addRelation = Props.get("add-relation").toBoolean
    val relationSyn = Props.get("relation-syn").toBoolean

    val mysql = new JdbcDF(spark)
    mysql.cacheRelationFromMysql

    if (addUser || addRelation) {
      if (addUser) {
        val titanInit = new TitanInit()
        titanInit.run()
        dataMigration.addUsers()
        titanInit.userIdUnique(titanInit.getTitanGraph)
      }
      if (addRelation) dataMigration.addRelations()
    }

    if (relationSyn) {
      dataMigration.relationsSyn()
    }

    SparkSession.clearActiveSession()
    println(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
    System.exit(0)
  }
}
