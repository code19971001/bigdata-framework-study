package com.itcode.join

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BigTableJoin {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("BigTableJoin")
      .set("spark.sql.shuffle.partitions", "36")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    useJoin(sparkSession)

  }


  def useJoin(sparkSession: SparkSession): Unit = {
    //查询出三张表 并进行join 插入到最终表中
    val saleCourse: DataFrame = sparkSession.sql("select *from spark_tuning.sale_course")
    val coursePay: DataFrame = sparkSession.sql("select * from spark_tuning.course_pay")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    val courseShoppingCart: DataFrame = sparkSession.sql("select *from spark_tuning.course_shopping_cart")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    courseShoppingCart
      .join(coursePay, Seq("orderid"), "left")
      .join(saleCourse, Seq("courseid"), "right")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "spark_tuning.sale_course.dt", "spark_tuning.sale_course.dn")
      .write.mode(SaveMode.Overwrite).saveAsTable("spark_tuning.salecourse_detail_1")

  }

}
