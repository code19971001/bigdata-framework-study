package com.itcode.skew

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.itcode.entity.{CourseShoppingCart, SaleCourse}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SkewJoinTuning {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SkewJoinTuning")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "36")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    scatterBigAndExpansionSmall(sparkSession)

    while (true) {}
  }


  /**
   * 打散大表  扩容小表 解决数据倾斜
   *
   */
  def scatterBigAndExpansionSmall(sparkSession: SparkSession): Unit = {
    val saleCourse: DataFrame = sparkSession.sql("select *from spark_tuning.sale_course")
    val coursePay: DataFrame = sparkSession.sql("select * from spark_tuning.course_pay")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    val courseShoppingCart: DataFrame = sparkSession.sql("select * from spark_tuning.course_shopping_cart")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    // TODO 1、拆分 倾斜的key
    val commonCourseShoppingCart: Dataset[Row] = courseShoppingCart.filter(item => item.getAs[Long]("courseid") != 101 && item.getAs[Long]("courseid") != 103)
    val skewCourseShoppingCart: Dataset[Row] = courseShoppingCart.filter(item => item.getAs[Long]("courseid") == 101 || item.getAs[Long]("courseid") == 103)

    //TODO 2、将倾斜的key打散  打散36份
    import sparkSession.implicits._
    val newCourseShoppingCart: Dataset[CourseShoppingCart] = skewCourseShoppingCart.mapPartitions((partitions: Iterator[Row]) => {
      partitions.map(item => {
        val courseid: Long = item.getAs[Long]("courseid")
        val randInt: Int = Random.nextInt(36)
        CourseShoppingCart(courseid, item.getAs[String]("orderid"),
          item.getAs[String]("coursename"), item.getAs[String]("cart_discount"),
          item.getAs[String]("sellmoney"), item.getAs[String]("cart_createtime"),
          item.getAs[String]("dt"), item.getAs[String]("dn"), randInt + "_" + courseid)
      })
    })
    //TODO 3、小表进行扩容 扩大36倍
    import sparkSession.implicits._
    val newSaleCourse: Dataset[SaleCourse] = saleCourse.flatMap(item => {
      val list = new ArrayBuffer[SaleCourse]()
      val courseid: Long = item.getAs[Long]("courseid")
      val coursename: String = item.getAs[String]("coursename")
      val status: String = item.getAs[String]("status")
      val pointlistid: Long = item.getAs[Long]("pointlistid")
      val majorid: Long = item.getAs[Long]("majorid")
      val chapterid: Long = item.getAs[Long]("chapterid")
      val chaptername: String = item.getAs[String]("chaptername")
      val edusubjectid: Long = item.getAs[Long]("edusubjectid")
      val edusubjectname: String = item.getAs[String]("edusubjectname")
      val teacherid: Long = item.getAs[Long]("teacherid")
      val teachername: String = item.getAs[String]("teachername")
      val coursemanager: String = item.getAs[String]("coursemanager")
      val money: String = item.getAs[String]("money")
      val dt: String = item.getAs[String]("dt")
      val dn: String = item.getAs[String]("dn")
      for (i <- 0 until 36) {
        list.append(SaleCourse(courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
          edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, i + "_" + courseid))
      }
      list
    })

    // TODO 4、倾斜的大key 与  扩容后的表 进行join
    val df1: DataFrame = newSaleCourse
      .join(newCourseShoppingCart.drop("courseid").drop("coursename"), Seq("rand_courseid", "dt", "dn"), "right")
      .join(coursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")


    // TODO 5、没有倾斜大key的部分 与 原来的表 进行join
    val df2: DataFrame = saleCourse
      .join(commonCourseShoppingCart.drop("coursename"), Seq("courseid", "dt", "dn"), "right")
      .join(coursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")

    // TODO 6、将 倾斜key join后的结果 与 普通key join后的结果，uinon起来
    df1
      .union(df2)
      .write.mode(SaveMode.Overwrite).insertInto("spark_tuning.salecourse_detail")
  }


}
