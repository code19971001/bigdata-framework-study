package com.itcode.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/21 23:40
 */
object Rdd_Persist {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd_Persist")
    val sc = new SparkContext(sparkConfig)
    val list = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatRdd.map(word => {
      println("--------------")
      (word, 1)
    })
    //因为mapRdd需要被重用，而RDD中是不存储数据，为了避免重复执行，可以先执行持久化操作。
    //mapRdd.cache()：存放到呃逆村中，实际上cache也是调用的persist,持久化操作必须再行动算子执行时完成的，也就是说只有需要行动算子来触发执行。
    mapRdd.persist()
    val result: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    result.collect().foreach(println)
    println("=======================")
    val groupRdd: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()
    groupRdd.collect().foreach(println)
    sc.stop()
  }

}
