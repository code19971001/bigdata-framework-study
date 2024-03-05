package com.itcode.rdd.operator.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * list all ips from apache log
 *
 * @author : code1997
 */
object GetIpFromApacheLog {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("apache-log")
    val sc = new SparkContext(sparkConf)
    val data: RDD[String] = sc.textFile("spark-framework/spark-core/src/main/resources/data/logs/apache.log")
    val ips: RDD[String] = data.map(line => {
      line.split(" ")(0)
    })
    ips.coalesce(1).distinct().saveAsTextFile("spark-framework/spark-core/src/main/resources/data/logs/ips")
    sc.stop()
  }

}
