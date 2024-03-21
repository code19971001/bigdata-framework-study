package com.itcode.job

import com.itcode.entity.CoursePay
import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object AckWaitTuning {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AckWaitTuning")
    //      .set("spark.core.connection.ack.wait.timeout", "2s") // 连接超时时间，默认等于spark.network.timeout的值，默认120s
    //          .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    useOFFHeapMemory(sparkSession)
  }

  def useOFFHeapMemory(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val result: Dataset[CoursePay] = sparkSession.sql("select * from spark_tuning.course_pay").as[CoursePay]
    result.cache()
    result.foreachPartition((p: Iterator[CoursePay]) => p.foreach(item => println(item.orderid)))

    //    while (true) {}
  }
}
