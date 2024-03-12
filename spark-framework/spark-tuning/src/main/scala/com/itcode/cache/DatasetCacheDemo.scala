package com.itcode.cache

import com.itcode.utils.InitUtil
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DatasetCacheDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DatasetCacheDemo")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    val result: DataFrame = sparkSession.sql("select * from spark_tuning.course_pay")
    result.cache()
    result.foreachPartition((p: Iterator[Row]) => p.foreach(item => println(item.get(0))))
    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让spark context立马结束
    }
  }

}
