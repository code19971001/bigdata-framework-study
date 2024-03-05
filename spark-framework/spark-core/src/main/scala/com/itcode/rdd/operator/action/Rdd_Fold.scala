package com.itcode.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * fold:当分区内和分区之间的规则相同的时候，可以使用fold来替换aggregate
 *
 * @author : code1997
 * @date : 2022/2/18 11:23
 */
object Rdd_Fold {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //这是一个行动算子，注意和AggregateByKey的区别.
    //AggregateByKey：初始值仅仅会参与分区内的计算
    //Aggregate：初始值会参与分区内和分区之间的计算=>10+13+17
    val result: Int = data.fold(10)(_ + _)
    println(result)
    sc.stop()
  }

}
