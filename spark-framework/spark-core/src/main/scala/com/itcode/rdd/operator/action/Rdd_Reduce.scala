package com.itcode.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 将数据源中的数据两两聚合，计算出结果
 *
 * @author : code1997
 * @date : 2022/2/18 9:49
 */
object Rdd_Reduce {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Int] = sc.makeRDD(List(4, 2, 1, 3))
    //两两聚合
    val sum: Int = data.reduce(_ + _)
    println(sum)
    //求出count
    val count: Long = data.count()
    println(count)
    //求数据源中的第一个
    val firstEle: Int = data.first()
    println(firstEle)
    //取几个，按数据的顺序
    val some: Array[Int] = data.take(2)
    println(some.mkString(","))
    //默认为升序
    val orderedEle: Array[Int] = data.takeOrdered(3)
    println(orderedEle.mkString(","))
    val orderedEle1: Array[Int] = data.takeOrdered(3)(Ordering.Int.reverse)
    println(orderedEle1.mkString(","))
    sc.stop()

  }
}
