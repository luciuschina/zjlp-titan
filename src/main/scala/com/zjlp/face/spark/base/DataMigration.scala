package com.zjlp.face.spark.base

import java.util

import com.zjlp.face.bean.{UsernameVID, Relation}
import com.zjlp.face.titan.{EsDAOImpl, TitanInit, TitanDAOImpl, TitanDAO}
import org.apache.spark.Logging
import scala.collection.JavaConversions._

/**
 * Created by root on 10/12/16.
 */
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
    sqlContext.sql(
      s"CREATE TEMPORARY TABLE relInES " +
        s"USING org.elasticsearch.spark.sql " +
        s"OPTIONS (resource 'titan-es/rel', es.read.metadata 'true')")
    sqlContext.sql("SELECT _metadata._id as usernameInES, vertexId FROM relInES")
      .registerTempTable("usernameVertexIdMap")
    sqlContext.sql("cache table usernameVertexIdMap")
  }


  def addUsers() = {
    MySQLContext.instance().sql("select username from relation union select loginAccount from relation ")
      .map(r => r(0).toString).distinct().foreachPartition {
      usernameRDD =>
        val titanDao = new TitanDAOImpl()
        val esDao = new EsDAOImpl()
        val list: util.List[UsernameVID] = new util.ArrayList[UsernameVID]()
        usernameRDD.foreach {
          username => list.add(new UsernameVID(username, titanDao.addUser(username, true)))
        }
        titanDao.getGraphTraversal.tx().commit();
        titanDao.closeTitanGraph();
        esDao.multiCreate(list);
    }
  }

  def addRelations() = {
    getUsernameVertexIdFromES
    val beginTime = System.currentTimeMillis()
    MySQLContext.instance().sql("select usernameVID,vertexId as loginAccountVID from  (select vertexId as usernameVID,loginAccount from relation inner join usernameVertexIdMap on usernameInES = username) b inner join usernameVertexIdMap on usernameInES = loginAccount")
      .map(r => (r(0).toString, r(1).toString)).distinct().foreachPartition {
      pairRDDs =>
        val titanDao = new TitanDAOImpl()
        var count = 0
        pairRDDs.foreach {
          pairRDD =>
            titanDao.addRelationByVID(pairRDD._1, pairRDD._2, false)
            count = count + 1
            if (count % 1000 == 0) titanDao.getGraphTraversal.tx().commit()
        }
        titanDao.getGraphTraversal.tx().commit()
        titanDao.closeTitanGraph();
    }
    logInfo(s"addRelations 耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
  }

  def addRelationsOld() = {
    MySQLContext.instance().sql("select username,loginAccount from relation ")
      .map(r => (r(0).toString, r(1).toString)).groupByKey().foreachPartition {
      relRDD =>
        val titanDao: TitanDAO = new TitanDAOImpl()
        relRDD.foreach { a =>
          logInfo(s"add relations for user:${a._1}")
          titanDao.addRelationsByUsername(a._1, a._2.toList);
        }
        titanDao.closeTitanGraph();
    }
  }


  def clearAndInit(): Unit = {
    val ti: TitanInit = new TitanInit()
    ti.cleanTitanGraph
    ti.createVertexLabel
    ti.createEdgeLabel
    //ti.createIndex
    ti.closeTitanGraph
  }

}
