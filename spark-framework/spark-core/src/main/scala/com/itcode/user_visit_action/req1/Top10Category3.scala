package com.itcode.user_visit_action.req1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10Category3 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    val flatRdd: RDD[(String, (Int, Int, Int))] = sourceData.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val resultRdd: RDD[(String, (Int, Int, Int))] = flatRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    //按照(点击数量，下单数量，支付数量)的值降序排列
    val top10Rdd: Array[(String, (Int, Int, Int))] = resultRdd.sortBy(_._2, ascending = false).take(10)
    //6.采集结果到控制台打印出来.
    top10Rdd.foreach(println)
    val end = System.currentTimeMillis()
    printf("花费的时间为:%d", end - start)
    sc.stop()
  }

}
