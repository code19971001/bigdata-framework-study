package com.itcode.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author : code1997
 * @date : 2022/2/24 23:33
 */
object Rdd_BroadCast {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    //join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRdd.collect().foreach(println)
    println("=========================")
    //使用map的方式就不会进行shuffle操作，但是假设我们有10个分区，但是只有1个executor,map又需要在每个分区上存在，那么就相当于冗余了10份，占用大量的内存
    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    rdd1.map {
      case (w, c) => {
        val l: Int = map.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)
    println("=========================")
    //改进：使用广播只读变量,executor实际上是一个JVM,所以在启动的时候，会自动分配内存，所以可以以executor为单位，将数据放到内存中，保存一份只读变量，降低冗余.
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(mutable.Map(("a", 4), ("b", 5), ("c", 6)))
    rdd1.map {
      case (w, c) =>
        (w, (c, bc.value.getOrElse(w, 0)))
    }.collect().foreach(println)
  }

}
