package com.itcode.cache

import com.itcode.entity.CoursePay
import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object RddCacheWithKryoDemo {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("RddCacheWithKryoDemo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CoursePay]))
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    //导入隐式转换
    import sparkSession.implicits._
    val result: RDD[CoursePay] = sparkSession.sql("select * from spark_tuning.course_pay").as[CoursePay].rdd
    result.cache()
    result.foreachPartition((p: Iterator[CoursePay]) => p.foreach(item => println(item.orderid)))
    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让spark context立马结束
    }
  }

}
