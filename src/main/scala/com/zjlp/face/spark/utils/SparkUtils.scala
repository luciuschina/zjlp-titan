package com.zjlp.face.spark.utils

import org.apache.spark.sql.SparkSession

object SparkUtils extends Serializable {
  def firstItrAsDouble(iter: Iterable[Double], defaultValue: Any = None) = {
    if (iter.isEmpty) {
      defaultValue
    } else {
      retainDecimal(iter.head, 2)
    }
  }

  def dropTempTable(spark: SparkSession, tableName: String) = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  def dropTempTables(spark: SparkSession, tables: String*) = {
    tables.foreach {
      table => dropTempTable(spark, table)
    }
  }

  def firstItrAsInt(iter: Iterable[Int], defaultValue: Any = None) = {
    if (iter.isEmpty) {
      defaultValue
    } else {
      iter.head
    }
  }

  def firstItr[T](iter: Iterable[T], defaultValue: T = None): T = {
    if (iter.isEmpty) {
      defaultValue
    } else {
      iter.head
    }
  }

  def toProduct[A <: Object](seq: Seq[A]) =
    Class.forName("scala.Tuple" + seq.size).getConstructors.apply(0).newInstance(seq: _*).asInstanceOf[Product]

  def trimIterable[A <: Iterable[String]](iterable: A): A = {
    iterable.map(_.trim).asInstanceOf[A]
  }

  def trimTuple(x: Product) = toProduct((for (e <- x.productIterator) yield {
    e.toString.trim
  }).toList)

  /**
   * 保留小数位数
   */
  def retainDecimal(number: Double, bits: Int = 2): Double = {
    BigDecimal(number).setScale(bits, BigDecimal.RoundingMode.HALF_UP).doubleValue()
  }

  def retainDecimalDown(number: Double, bits: Int = 2): Double = {
    BigDecimal(number).setScale(bits, BigDecimal.RoundingMode.DOWN).doubleValue()
  }

}


