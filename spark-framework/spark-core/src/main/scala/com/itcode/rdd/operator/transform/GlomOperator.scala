package com.itcode.rdd.operator.transform

import com.itcode.rdd.utils.SparkUtil

/**
 * 分区确定，当数据经过转换之后，分区是不会发生变化的.
 * 将分区数据转化为一个数组.
 * 从int => 数组
 *
 * @author : code1997
 */
object GlomOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtil.createSparkContext("GlomOperator")
    //int==>array
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    val glomRDD1: RDD[Array[Int]] = rdd.glom()
    //List[Array[int]]
    glomRDD1.collect().foreach(data => println(data.mkString(",")))

    val glomRDD2: RDD[Array[Int]] = rdd.glom()
    val maxValues: RDD[Int] = glomRDD2.map(arr => {
      arr.max
    })
    println(maxValues.collect().sum)
    sc.stop()
  }

}
