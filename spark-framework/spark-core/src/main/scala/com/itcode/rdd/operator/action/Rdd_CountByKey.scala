package com.itcode.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author : code1997
 * @date : 2022/2/18 11:27
 */
object Rdd_CountByKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //处理单值类型
    val map: collection.Map[Int, Long] = data.countByValue()
    //Map(4 -> 1, 1 -> 1, 2 -> 1, 3 -> 1)
    println(map)
    val data2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 5),("a",3)))
    val map1: collection.Map[String, Long] = data2.countByKey()
    //Map(a -> 2, b -> 1, c -> 1)
    println(map1)
    sc.stop()
  }

}
