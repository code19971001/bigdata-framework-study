package com.itcode.user_visit_action.req2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10CategorySession1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc: SparkContext = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    sourceData.cache()
    val top10Ids: Array[String] = top10Category(sourceData)
    //过滤原始数据，保留点击和前10品类的id
    val filterdClickActionRdd: RDD[String] = sourceData.filter(
      action => {
        val datas: Array[String] = action.split("_")
        //只要top10的数据
        datas(6) != "-1" && top10Ids.contains(datas(6))
      }
    )
    //根据(品类id,session id)进行点击量的统计
    val result: RDD[(String, List[(String, Int)])] = filterdClickActionRdd.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _).map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }.groupByKey() //相同的品类分到一组
      .mapValues(
        iter => {
          iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
        }
      )
    result.foreach(println)
    val end = System.currentTimeMillis()
    printf("花费的时间为:%d", end - start)
    sc.stop()
  }

  /**
   * 获取热度top10的ids
   */
  def top10Category(sourceData: RDD[String]): Array[String] = {
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
    resultRdd.sortBy(_._2, ascending = false).take(10).map(_._1)
  }

}
