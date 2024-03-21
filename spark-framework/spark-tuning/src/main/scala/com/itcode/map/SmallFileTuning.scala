package com.itcode.map

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SmallFileTuning {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("MapSmallFileTuning")
      .set("spark.files.openCostInBytes", "7194304") //默认4m
      .set("spark.sql.files.maxPartitionBytes", "128MB") //默认128M
      .setMaster("local[1]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    sparkSession.sql("select * from spark_tuning.course_shopping_cart")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("spark_tuning.course_shopping_cart_merge")


    while (true) {}
  }

}
