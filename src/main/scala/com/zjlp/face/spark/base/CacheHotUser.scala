package com.zjlp.face.spark.base

import com.zjlp.face.titan.TitanConPool

object CacheHotUser extends scala.Serializable {
  def main(args: Array[String]) {
    new TitanConPool().killAllTitanInstances()
  }
}
