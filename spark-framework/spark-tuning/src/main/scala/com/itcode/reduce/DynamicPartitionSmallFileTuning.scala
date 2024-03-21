package com.itcode.reduce

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DynamicPartitionSmallFileTuning {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DynamicPartitionSmallFileTuning")
      .set("spark.sql.shuffle.partitions", "36")
      .set("spark.sql.adaptive.enabled","false")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    //sparkSession.sql("ALTER TABLE spark_tuning.course_shopping_cart DROP column (_corrupt_record)  ")
    sparkSession.sql("select * from spark_tuning.course_shopping_cart").drop("_corrupt_record").createTempView("course_shopping_cart_view")

//    sparkSession.sql(
//      """
//        |CREATE TABLE if not exists `spark_tuning`.`dynamic_csc` (
//        |  `courseid` BIGINT,
//        |  `coursename` STRING,
//        |  `createtime` STRING,
//        |  `discount` STRING,
//        |  `orderid` STRING,
//        |  `sellmoney` STRING,
//        |  `dt` STRING,
//        |  `dn` STRING)
//        |USING parquet
//        |PARTITIONED BY (dt, dn)
//          """.stripMargin)
//
    // TODO 非倾斜分区写入
    sparkSession.sql(
      """
        |insert overwrite spark_tuning.dynamic_csc partition(dt,dn)
        |select * from course_shopping_cart_view
        |where dt!='20190722' and dn!='webA'
        |distribute by dt,dn
      """.stripMargin)

    // TODO 倾斜分区打散写入
    sparkSession.sql(
      """
        |insert overwrite spark_tuning.dynamic_csc partition(dt,dn)
        |select * from course_shopping_cart_view
        |where dt='20190722' and dn='webA'
        |distribute by cast(rand() * 5 as int)
      """.stripMargin)


    while (true) {}
  }

}
