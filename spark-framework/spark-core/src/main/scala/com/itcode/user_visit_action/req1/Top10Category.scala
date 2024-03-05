package com.itcode.user_visit_action.req1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10Category {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    //Q1：sourceData读取三次，可以使用cache
    sourceData.cache()
    //2.统计品类的点击数量：(品类ID，点击数量)
    val clickActionRdd: RDD[(String, Int)] = sourceData.filter(
      (action: String) => {
        action.split("_")(6) != "-1"
      }
    ).map((action: String) => {
      (action.split("_")(6), 1)
    }).reduceByKey((_: Int) + (_: Int))

    //3.统计品类的下单数量：(品类ID，下单数量)
    val orderActionRdd: RDD[(String, Int)] = sourceData.filter(
      (action: String) => {
        action.split("_")(8) != "null"
      }
    ).flatMap((action: String) => {
      action.split("_")(8).split(",")
    }).map((_: String, 1)).reduceByKey((_: Int) + (_: Int))

    //4.统计品类的下单数量：(品类ID，支付数量)
    val payActionRdd: RDD[(String, Int)] = sourceData.filter(
      (action: String) => {
        action.split("_")(10) != "null"
      }
    ).flatMap((action: String) => {
      action.split("_")(10).split(",")
    }).map((_: String, 1)).reduceByKey((_: Int) + (_: Int))
    //5.将品类进行排序，并且，取前10名(点击数量，下单数量，支付数量)，联想到元组的排序
    //将key相同的marge到一块
    //join，leftJoin?不行，我们需要(点击数量，下单数量，支付数量)三个都有值，有些品类可能是没有这些活动中的一项，会导致缺项的事情发生
    //cogroup：connect+group
    //todo:需要理解一下
    val categoryVisitRdd: RDD[(String, (Int, Int, Int))] = clickActionRdd
      .cogroup(orderActionRdd, payActionRdd)
      .mapValues {
        case (clickIter, orderIter, payIter) =>
          var clickCount = 0
          val clickIterator: Iterator[Int] = clickIter.iterator
          if (clickIterator.hasNext) {
            clickCount = clickIterator.next()
          }
          var orderCount = 0
          val orderIterator: Iterator[Int] = orderIter.iterator
          if (orderIterator.hasNext) {
            orderCount = orderIterator.next()
          }
          var payCount = 0
          val payIterator: Iterator[Int] = payIter.iterator
          if (payIterator.hasNext) {
            payCount = payIterator.next()
          }
          (clickCount, orderCount, payCount)
      }
    //按照(点击数量，下单数量，支付数量)的值降序排列
    val top10Rdd: Array[(String, (Int, Int, Int))] = categoryVisitRdd.sortBy(_._2, ascending = false).take(10)
    //6.采集结果到控制台打印出来.
    top10Rdd.foreach(println)
    val end = System.currentTimeMillis()
    printf("花费的时间为:%d", end - start)
    sc.stop()

  }

}
