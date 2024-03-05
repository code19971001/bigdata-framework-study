package com.itcode.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/24 22:29
 */
object Rdd_Accumulator {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    val sourceData: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //方式1：使用reduce
    println(sourceData.reduce(_+_))
    //方式2：遍历相加即可，相加是在driver端执行的，不会将结果返回回来
    var sum = 0
    sourceData.foreach(num => {
      sum += num
    })
    //sum = 0
    println("sum = " + sum)
    //正确的方式：使用累加器，可以在executor计算完成之后，再将结果返回给driver
    val sum1: LongAccumulator = sc.longAccumulator("sum")
    sourceData.foreach(num => {
      sum1.add(num)
    })
    //sum = 0
    println("result = " + sum1.value)
    sc.stop()
  }

}
