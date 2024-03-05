package com.itcode.rdd.operator.transform

/**
 *
 * @author : code1997
 * @date : 2021/9/25 8:28
 */
object SampleOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd-file")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //抽取数据不放回
    println(rdd.sample(false, 0.4, 1).collect().mkString(","))
    sc.stop()
  }

}
