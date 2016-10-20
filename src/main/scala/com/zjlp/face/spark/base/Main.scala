package com.zjlp.face.spark.base

import com.zjlp.face.titan.TitanInit
import org.apache.spark.Logging

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends Logging {
  def main(args: Array[String]) {
    val beginTime = System.currentTimeMillis()
    val sql = new DataMigration()
    val titanInit = new TitanInit()

    sql.getRelationFromMySqlDB

    if (Props.get("add-user").toBoolean) {
      titanInit.run()
      sql.addUsers()
      titanInit.usernameUnique()
      titanInit.closeTitanGraph()
    }

    if (Props.get("add-relation").toBoolean) sql.addRelations()

    MySparkContext.instance().stop()
    logInfo(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
  }
}
