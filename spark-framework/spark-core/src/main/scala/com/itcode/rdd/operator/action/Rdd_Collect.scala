package com.itcode.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * 所谓的行动算子是可以触发job执行的算子
 *
 * @author : code1997
 * @date : 2022/2/18 9:49
 */
object Rdd_Collect {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    data.collect()
    sc.stop()

  }
}
