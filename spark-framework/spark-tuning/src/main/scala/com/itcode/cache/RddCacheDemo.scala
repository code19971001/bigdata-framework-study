package com.itcode.cache

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object RddCacheDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("RddCacheDemo")
    .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    val result: RDD[Row] = sparkSession.sql("select * from spark_tuning.course_pay").rdd
    result.cache()
    result.foreachPartition((p: Iterator[Row]) => p.foreach(item => println(item.get(0))))
    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让spark context立马结束
    }
  }

}
