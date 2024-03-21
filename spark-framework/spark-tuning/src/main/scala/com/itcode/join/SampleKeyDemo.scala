package com.itcode.join

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SampleKeyDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SampleKeyDemo")
      .set("spark.sql.shuffle.partitions", "36")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    println("=============================================csc courseid sample=============================================")
    val cscTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession, "spark_tuning.course_shopping_cart", "courseid")
    println(cscTopKey.mkString("\n"))

    println("=============================================sc courseid sample=============================================")
    val scTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession, "spark_tuning.sale_course", "courseid")
    println(scTopKey.mkString("\n"))

    println("=============================================cp orderid sample=============================================")
    val cpTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession, "spark_tuning.course_pay", "orderid")
    println(cpTopKey.mkString("\n"))

    println("=============================================csc orderid sample=============================================")
    val cscTopOrderKey: Array[(Int, Row)] = sampleTopKey(sparkSession, "spark_tuning.course_shopping_cart", "orderid")
    println(cscTopOrderKey.mkString("\n"))
  }


  def sampleTopKey(sparkSession: SparkSession, tableName: String, keyColumn: String): Array[(Int, Row)] = {
    val df: DataFrame = sparkSession.sql("select " + keyColumn + " from " + tableName)
    val top10Key: Array[(Int, Row)] = df
      .select(keyColumn).sample(withReplacement = false, 0.4).rdd // 对key不放回采样
      .map(k => (k, 1)).reduceByKey(_ + _) // 统计不同key出现的次数
      .map(k => (k._2, k._1)).sortByKey(ascending = false) // 统计的key进行排序
      .take(10)
    top10Key
  }
}
