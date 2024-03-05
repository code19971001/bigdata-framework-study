package com.itcode.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/24 22:58
 */
object Rdd_Accumulator_Exception {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    val sourceData: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val mapRdd: RDD[Int] = sourceData.map(num => {
      sum.add(num)
      num
    })
    mapRdd.collect()
    mapRdd.collect()
    //20
    println(sum)


    sc.stop()


  }

}
