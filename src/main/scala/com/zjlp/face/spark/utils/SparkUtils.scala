package com.zjlp.face.spark.utils

import java.util

import com.zjlp.face.spark.base.MySQLContext
import org.apache.spark.sql.SQLContext


object SparkUtils extends Serializable {
  def firstItrAsDouble(iter: Iterable[Double], defaultValue: Any = None) = {
    if (iter.isEmpty) {
      defaultValue
    } else {
      retainDecimal(iter.head, 2)
    }
  }

  def dropTempTable(sqlContext:SQLContext,table:String) = {
    if(sqlContext.tableNames().contains(table)) sqlContext.dropTempTable(table)
  }
  def dropTempTables(sqlContext:SQLContext,tables:String*) = {
    tables.foreach{
      table => dropTempTable(sqlContext,table)
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

  def itrToJavaList[T](itr:Iterator[T]): util.List[T] = {
    val list = new util.ArrayList[T]()
    while (itr.hasNext) {
      list.add(itr.next())
    }
    list
  }

  def getLastTable(tablePrefix: String): String = {
    MySQLContext.instance().tableNames().filter(_.startsWith(tablePrefix)).sorted.reverse(0)
  }


}


