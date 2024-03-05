package com.itcode.rdd.operator.transform

/**
 * @author : code1997
 * @date : 2021/9/25 8:58
 */
object DistinctOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3))
    val distinctedData: RDD[Int] = data.distinct()
    distinctedData.collect().foreach(println)
    distinctedData.repartition(3)
    sc.stop()
  }

}
