package com.itcode.user_visit_action.req1

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10Category4 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    //注册累加器
    val top10Accumulator = new Top10CategoryAccumulator
    sc.register(top10Accumulator)
    sourceData.foreach(
      (action: String) => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          top10Accumulator.add(datas(6), "click")
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            (id: String) => {
              top10Accumulator.add(id, "order")
            }
          )
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(
            (id: String) => {
              top10Accumulator.add(id, "pay")
            }
          )
        } else {
          Nil
        }
      }
    )
    val categories: mutable.Iterable[HotCategory] = top10Accumulator.value.map((_: (String, HotCategory))._2)
    categories.toList.sortWith(
      (l: HotCategory, r: HotCategory) => {
        if (l.clickCnt > r.clickCnt) {
          true
        } else if (l.orderCnt == r.orderCnt) {
          if (l.orderCnt > r.orderCnt) {
            true
          } else if (l.orderCnt == r.orderCnt) {
            l.payCnt >= r.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10).foreach(println)
    val end = System.currentTimeMillis()
    printf("花费的时间为:%d", end - start)
    sc.stop()
  }

  case class HotCategory(id: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int) {

  }

  /**
   * 自定义累加器
   */
  class Top10CategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val top10Map: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = top10Map.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new Top10CategoryAccumulator()

    override def reset(): Unit = top10Map.clear()

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2
      val category: HotCategory = top10Map.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      top10Map.update(cid, category)
    }


    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1: mutable.Map[String, HotCategory] = this.top10Map
      val map2: mutable.Map[String, HotCategory] = other.value
      map2.foreach {
        case (cid, hc) =>
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          top10Map.put(cid, category)
      }
    }

    override def value: mutable.Map[String, HotCategory] = top10Map
  }

}
