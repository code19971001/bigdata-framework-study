package com.itcode.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 自定义累加器实现word count
 *
 * @author : code1997
 * @date : 2022/2/24 23:06
 */
object Rdd_Accumulator_WC {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    val data: RDD[String] = sc.makeRDD(List("hello", "java", "hello", "scala"))
    val accumulator = new MyAccumulator
    sc.register(accumulator, "wordCountAcc")
    data.foreach(word => {
      accumulator.add(word)
    })
    println(accumulator.value)
    sc.stop()
  }

  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    /**
     * 用于存储累加器中的数据
     */
    private val map: mutable.Map[String, Long] = mutable.Map[String, Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()

    override def reset(): Unit = map.clear()

    override def add(v: String): Unit = {
      val count: Long = map.getOrElse(v, 0L) + 1
      map.put(v, count)
    }

    //合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = map
      val map2 = other.value
      map2.foreach {
        case (word, count) => {
          val newCount: Long = map1.getOrElse(word, 0L) + count
          map1.put(word, newCount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = map
  }

}
