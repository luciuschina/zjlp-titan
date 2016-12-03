package com.zjlp.face.spark.base

import org.apache.spark.sql.SparkSession

object Spark {
  def session(): SparkSession = {
    return SparkSession.builder()
      .appName(Props.get("spark.app.name"))
      .config("spark.master", Props.get("spark.master"))
      .config("spark.sql.shuffle.partitions", Props.get("spark.sql.shuffle.partitions"))
      .config("spark.executor.memory", Props.get("spark.executor.memory"))
      .config("spark.executor.cores", Props.get("spark.executor.cores"))
      .config("spark.driver.memory", Props.get("spark.driver.memory"))
      .config("spark.driver.cores", Props.get("spark.driver.cores"))
      .config("spark.default.parallelism", Props.get("spark.default.parallelism"))
      .config("spark.jars", Props.get("spark.jars"))
      .config("es.nodes", Props.get("es.nodes"))
      .config("es.port", Props.get("es.port"))
      .config("pushdown", Props.get("pushdown"))
      .config("strict", Props.get("strict"))
      .getOrCreate()
  }
}

