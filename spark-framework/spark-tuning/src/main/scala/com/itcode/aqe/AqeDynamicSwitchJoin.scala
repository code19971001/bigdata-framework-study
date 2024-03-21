package com.itcode.aqe

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AqeDynamicSwitchJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AqeDynamicSwitchJoin")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.localShuffleReader.enabled", "true") //在不需要进行shuffle重分区时，尝试使用本地shuffle读取器。将sort-meger join 转换为广播join
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    switchJoinStartegies(sparkSession)
  }


  def switchJoinStartegies(sparkSession: SparkSession): Unit = {
    val coursePay: Dataset[Row] = sparkSession.sql("select * from spark_tuning.course_pay")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
      .where("orderid between 'odid-9999000' and 'odid-9999999'")
    val courseShoppingCart: DataFrame = sparkSession.sql("select *from spark_tuning.course_shopping_cart")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val tmpdata: DataFrame = coursePay.join(courseShoppingCart, Seq("orderid"), "right")
    tmpdata.show()
  }
}
