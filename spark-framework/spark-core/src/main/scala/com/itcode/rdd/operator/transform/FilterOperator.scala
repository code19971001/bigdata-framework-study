package com.itcode.rdd.operator.transform

/**
 * @author : code1997
 * @date : 2021/9/23 23:59
 */
object FilterOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    data.filter(data => data % 2 != 0).collect().foreach(println)

    val lines: RDD[String] = sc.textFile("data/spark-core/operator/apache.log")
    lines.filter(line => {
      val strs: Array[String] = line.split(" ")
      strs(3).startsWith("17/05/2015")
    }).map(line => {
      val strs: Array[String] = line.split(" ")
      strs(6)
    }).collect().foreach(println)

    sc.stop()
  }

}
