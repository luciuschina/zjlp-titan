package com.zjlp.face.spark.base

import com.zjlp.face.titan.TitanInit
import org.apache.spark.Logging

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends Logging {
  def main(args: Array[String]) {
    val beginTime = System.currentTimeMillis()
    val dataMigration = new DataMigration()

    val addUser = Props.get("add-user").toBoolean
    val addRelation = Props.get("add-relation").toBoolean
    val relationSyn = Props.get("relation-syn").toBoolean
    val cleanTitanInstances = Props.get("clean-titan-instances").toBoolean

    val mysql = new MySQL()
    mysql.cacheRelationFromMysql

    if (addUser || addRelation) {
      if (addUser) {
        val titanInit = new TitanInit()
        titanInit.run()
        dataMigration.addUsers()
        titanInit.usernameUnique()
        titanInit.closeTitanGraph()
      }
      if (addRelation) dataMigration.addRelations()
    }

    if (relationSyn) {
      dataMigration.relationsSyn()
    }

    if (cleanTitanInstances) {
      val titanInit = new TitanInit()
      titanInit.killOtherTitanInstances()
      titanInit.closeTitanGraph()
    }

    MySparkContext.instance().stop()
    logInfo(s"共耗时:${(System.currentTimeMillis() - beginTime) / 1000}s")
    //System.exit(0)
  }
}
