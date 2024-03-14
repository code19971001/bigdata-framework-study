package com.itcode.utils

import com.itcode.entity.Student
import com.itcode.entity.School
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Random

object InitUtil {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("InitData")
    //TODO 要打包提交集群执行，注释掉
    // .setMaster("local[*]")
    val sparkSession: SparkSession = initSparkSession(sparkConf)
    initHiveTable(sparkSession)
    //initBucketTable(sparkSession)
    //saveData(sparkSession)
  }

  def initSparkSession(sparkConf: SparkConf): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "code1997")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://bigdata01:9000")
    sparkSession
  }

  def initHiveTable(spark: SparkSession): Unit = {
    spark.read.json("/origin_data/apps/spark-tuning/coursepay.log").write.partitionBy("dt", "dn").format("parquet").mode("overwrite").saveAsTable("spark_tuning.course_pay")

    spark.read.json("/origin_data/apps/spark-tuning/salecourse.log").write.partitionBy("dt", "dn").format("parquet").mode(SaveMode.Overwrite).saveAsTable("spark_tuning.sale_course")

    spark.read.json("/origin_data/apps/spark-tuning/courseshoppingcart.log").write.partitionBy("dt", "dn").format("parquet").mode(SaveMode.Overwrite).saveAsTable("spark_tuning.course_shopping_cart")

  }

  def initBucketTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/origin_data/apps/spark-tuning/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .bucketBy(5, "orderid")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("spark_tuning.course_pay_cluster")
    sparkSession.read.json("/origin_data/apps/spark-tuning/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .bucketBy(5, "orderid")
      .format("parquet")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("spark_tuning.course_shopping_cart_cluster")
  }

  def saveData(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => Student(item, "name" + item, random.nextInt(100), random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("spark_tuning.test_student")

    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => School(item, "school" + item, random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("spark_tuning.test_school")
  }
}