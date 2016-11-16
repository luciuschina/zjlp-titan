package com.zjlp.face.spark.base

import com.zjlp.face.spark.utils.SparkUtils
import com.zjlp.face.titan.TitanInit
import com.zjlp.face.titan.impl.TitanDAOImpl
import org.apache.spark.Logging
import scala.collection.JavaConversions._

class DataMigration extends Logging with scala.Serializable {

  private def getIdMapFromES = {
    logInfo("从ES加载userId-vertexId索引数据")
    val sqlContext = MySQLContext.instance()
    SparkUtils.dropTempTables(sqlContext, "userIdVIdInES", "userIdVIdMap")
    sqlContext.sql(
      s"CREATE TEMPORARY TABLE userIdVIdInES " +
        s"USING org.elasticsearch.spark.sql " +
        s"OPTIONS (resource '${Props.get("titan-es-index")}/rel', es.read.metadata 'true')")
    sqlContext.sql("SELECT _metadata._id as userIdInES, vertexId FROM userIdVIdInES")
      .registerTempTable("userIdVIdMap")
    sqlContext.sql("cache table userIdVIdMap")
  }

  def addUsers() = {
    MySQLContext.instance().sql("select distinct userId from relation")
      .map(r => r(0).toString).distinct().foreachPartition {
      userIdRDD =>
        val titanDao = new TitanDAOImpl()
        userIdRDD.foreach(userId => titanDao.addUser(userId,titanDao.getTitanGraph))
        titanDao.closeTitanGraph()
    }
  }

  def addRelations() = {
    getIdMapFromES
    val beginTime = System.currentTimeMillis()
    val sqlContext = MySQLContext.instance()
    sqlContext.sql("select distinct r2.userId as userId1,r1.userId as userId2 from relation r1 inner join (select distinct loginAccount,userId from relation)r2 on r2.loginAccount = r1.username").registerTempTable("relByUserId")
    sqlContext.sql("select userIdVId1,vertexId as userIdVId2 from  (select vertexId as userIdVId1,userId2 from relByUserId inner join userIdVIdMap on userIdInES = userId1) b inner join userIdVIdMap on userIdInES = userId2")
      .map(r => (r(0).toString, r(1).toString))
      .distinct()
      .foreachPartition {
      pairRDDs =>
        val titanDao: TitanDAOImpl = new TitanDAOImpl()
        pairRDDs.foreach {
          pairRDD =>
            titanDao.addRelationByVID(pairRDD._1, pairRDD._2,titanDao.getTitanGraph(0))
        }
        titanDao.getTitanGraph(0).tx().commit()
        titanDao.closeTitanGraph()
    }
    SparkUtils.dropTempTable(sqlContext,"relByUserId")
    logInfo(s"addRelations 耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
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
  def relationsSyn(): Unit = {
    val sqlContext = MySQLContext.instance()
    getIdMapFromES //1
    getVidRelationsFromTitan //2
    val relInTitan = sqlContext.sql("select userIdTitan, userIdInES as friendUserIdTitan " +
      "from (select userIdInES as userIdTitan ,friendVidTitan from VidRelTitan inner join userIdVIdMap " +
      "on vertexId = userVidTitan) a inner join userIdVIdMap on vertexId = friendVidTitan ")
      .map(r => (r(0).toString, r(1).toString)).persist()
    val relInMysql = sqlContext.sql("select distinct r2.userId as userId1,r1.userId as userId2 from relation r1 inner join (select distinct loginAccount,userId from relation)r2 on r2.loginAccount = r1.username")
      .map(r => (r(0).toString, r(1).toString)).persist()
    val titan = new TitanDAOImpl()
    val userInMysql = sqlContext.sql("select distinct userId from relation")
      .map(r => r(0).toString)
    val userInTitan = sqlContext.sql("select distinct userIdInES from userIdVIdMap").map(r => r(0).toString)
    userInMysql.subtract(userInTitan).collect().foreach(userId => titan.addUser(userId,titan.getTitanGraph)) //5
    relInTitan.subtract(relInMysql).collect().foreach(rel => titan.deleteRelation(rel._1, rel._2)) //6
    relInMysql.subtract(relInTitan).collect().foreach(rel => titan.addRelation(rel._1, rel._2)) //7
    relInTitan.unpersist()
    relInMysql.unpersist()
    SparkUtils.dropTempTable(sqlContext, "VidRelTitan")
    //titan.closeTitanGraph()
  }


  private def getVidRelationsFromTitan = {
    val sqlContext = MySQLContext.instance()
    import sqlContext.implicits._
    sqlContext.sql("select distinct vertexId from userIdVIdMap ")
      .map(r => r(0).toString).mapPartitions {
      vidRDD =>
        val titan = new TitanDAOImpl()
        vidRDD.flatMap {
          vid => titan.getAllFriendVIds(vid).map(friendId => (vid, friendId.toString))
        }
    }.toDF("userVidTitan", "friendVidTitan").registerTempTable("VidRelTitan")
  }
}

object DataMigration extends Logging with scala.Serializable {
  def main(args: Array[String]) {
    val beginTime = System.currentTimeMillis()
    val dataMigration = new DataMigration()

    val addUser = Props.get("add-user").toBoolean
    val addRelation = Props.get("add-relation").toBoolean
    val relationSyn = Props.get("relation-syn").toBoolean

    val mysql = new MySQL()
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

    MySparkContext.instance().stop()
    logInfo(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
    System.exit(0)
  }
}
