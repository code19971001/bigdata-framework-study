package com.itcode.job

import com.itcode.entity.CoursePay
import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object OFFHeapCache {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("OFFHeapCache")
      .set("spark.memory.enable.offheap.enable", "true")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    useOFFHeapMemory(sparkSession)
  }

  def useOFFHeapMemory(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val result: Dataset[CoursePay] = sparkSession.sql("select * from spark_tuning.course_pay").as[CoursePay]
    // TODO 指定持久化到 堆外内存
    result.persist(StorageLevel.OFF_HEAP)
    result.foreachPartition((p: Iterator[CoursePay]) => p.foreach(item => println(item.orderid)))

    while (true) {}
  }
}
