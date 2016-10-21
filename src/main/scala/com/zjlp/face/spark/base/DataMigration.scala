package com.zjlp.face.spark.base

import com.zjlp.face.spark.utils.SparkUtils
import com.zjlp.face.titan.impl.TitanDAOImpl
import org.apache.spark.Logging
import scala.collection.JavaConversions._


class DataMigration extends Logging with scala.Serializable {
  def getRelationFromMySqlDB = {
    MySQLContext.instance().read.format("jdbc").options(Map(
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

  private def getUsernameVertexIdFromES = {
    logInfo("从ES加载username-vertexId索引数据")
    val sqlContext = MySQLContext.instance()
    SparkUtils.dropTempTables(sqlContext, "relInES", "usernameVertexIdMap")
    sqlContext.sql(
      s"CREATE TEMPORARY TABLE relInES " +
        s"USING org.elasticsearch.spark.sql " +
        s"OPTIONS (resource '${Props.get("titan-es-index")}/rel', es.read.metadata 'true')")
    sqlContext.sql("SELECT _metadata._id as usernameInES, vertexId FROM relInES")
      .registerTempTable("usernameVertexIdMap")
    sqlContext.sql("cache table usernameVertexIdMap")
  }

  def addUsers() = {
    MySQLContext.instance().sql("select username from relation union select loginAccount from relation ")
      .map(r => r(0).toString).distinct().foreachPartition {
      usernameRDD =>
        val titanDao = new TitanDAOImpl()
        usernameRDD.foreach(titanDao.addUser(_))
        titanDao.closeTitanGraph()
    }
  }

  def addRelations() = {
    getUsernameVertexIdFromES
    val beginTime = System.currentTimeMillis()
    MySQLContext.instance().sql("select usernameVID,vertexId as loginAccountVID from  (select vertexId as usernameVID,loginAccount from relation inner join usernameVertexIdMap on usernameInES = username) b inner join usernameVertexIdMap on usernameInES = loginAccount")
      .map(r => (r(0).toString, r(1).toString)).distinct().foreachPartition {
      pairRDDs =>
        val titanDao: TitanDAOImpl = new TitanDAOImpl()
        var count = 0
        pairRDDs.foreach {
          pairRDD =>
            titanDao.addRelationByVID(pairRDD._1, pairRDD._2, false)
            count = count + 1
            if (count % 1000 == 0) titanDao.getGraphTraversal.tx().commit()
        }
        titanDao.getGraphTraversal.tx().commit()
        titanDao.closeTitanGraph()
    }
    logInfo(s"addRelations 耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
  }

  /**
   * 数据同步，即对比titan与mysql中的数据是否一致，如果不一致就增量更新为与mysql中的一致
   * 步骤：
   * 1、从ES中的titan-es索引中读取username和VID的对应关系
   * 2、分片使用g.V(vid).out('knows').id 得到好友关系，可以得到 userVID - friendVID的对应关系
   * 3、结合步骤1和步骤2 从Titan中得到现有 username - friendUsername 的对应关系
   * 4、从mysql中得到现有 username - friendUsername 的对应关系
   * 5、比较有哪些新增的用户。往Titan中添加这些用户
   * 6、(步骤3的好友关系) substract (步骤4的好友关系) = 需要从Titan中删除的好友关系
   * 7、(步骤4的好友关系) substract (步骤3的好友关系) = 需要往Titan中添加的好友关系
   */
  def relationsSyn(): Unit = {
    val sqlContext = MySQLContext.instance()
    getUsernameVertexIdFromES //1
    getVidRelationsFromTitan //2

    val relInTitan = sqlContext.sql("select usernameTitan, usernameInES as friendUsernameTitan " +
      "from (select usernameInES as usernameTitan ,friendVidTitan from VidRelTitan inner join usernameVertexIdMap " +
      "on vertexId = userVidTitan) a inner join usernameVertexIdMap on vertexId = friendVidTitan ")
      .map(r => (r(0).toString, r(1).toString)).persist() //3

    getRelationFromMySqlDB //4
    val relInMysql = sqlContext.sql("select username,loginAccount from relation")
        .map(r => (r(0).toString, r(1).toString)).persist()

    val titan = new TitanDAOImpl()

    val userInMysql = sqlContext.sql("select username from relation union select loginAccount from relation ")
      .map(r => r(0).toString).distinct()
    val userInTitan = relInTitan.flatMap(t => List(t._1, t._2)).distinct()
    userInMysql.subtract(userInTitan).collect().foreach(username => titan.addUser(username)) //5

    relInTitan.subtract(relInMysql).collect().foreach(rel => titan.deleteRelation(rel._1, rel._2)) //6
    relInMysql.subtract(relInTitan).collect().foreach(rel => titan.addRelation(rel._1, rel._2)) //7
    relInTitan.unpersist()
    relInMysql.unpersist()
    SparkUtils.dropTempTable(sqlContext, "VidRelTitan")
  }

  private def getVidRelationsFromTitan = {
    val sqlContext = MySQLContext.instance()
    import sqlContext.implicits._
    sqlContext.sql("select vertexId from usernameVertexIdMap ")
      .map(r => r(0).toString).mapPartitions {
      vidRDD =>
        val titan = new TitanDAOImpl()
        vidRDD.flatMap {
          vid => titan.getAllFriendVIDs(vid).map(friendId => (vid, friendId.toString))
        }
    }.toDF("userVidTitan", "friendVidTitan").registerTempTable("VidRelTitan")
  }
}
