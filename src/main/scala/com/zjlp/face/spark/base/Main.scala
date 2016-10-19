package com.zjlp.face.spark.base

import com.zjlp.face.titan.TitanInit
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val beginTime = System.currentTimeMillis()
    val sql = new DataMigration()
    val  titanInit = new TitanInit()

    if(Props.get("clear-init").toBoolean) titanInit.run()

    sql.getRelationFromMySqlDB

    if(Props.get("add-user").toBoolean) {
      sql.addUsers()
      titanInit.usernameUnique()
      titanInit.cleanTitanGraph()
    }

    if(Props.get("add-relation").toBoolean) sql.addRelations()
    MySparkContext.instance().stop()
    logInfo(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
  }
}
