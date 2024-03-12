package com.itcode.join

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ForceBroadcastJoinTuning {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ForceBroadcastJoinTuning")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") // 关闭自动广播
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    //TODO SQL Hint方式
    val sqlStr1: String =
      """
        |select /*+  BROADCASTJOIN(sc) */
        |  sc.courseid,
        |  csc.courseid
        |from sale_course sc join course_shopping_cart csc
        |on sc.courseid=csc.courseid
      """.stripMargin

    val sqlStr2: String =
      """
        |select /*+  BROADCAST(sc) */
        |  sc.courseid,
        |  csc.courseid
        |from sale_course sc join course_shopping_cart csc
        |on sc.courseid=csc.courseid
      """.stripMargin

    val sqlStr3: String =
      """
        |select /*+  MAPJOIN(sc) */
        |  sc.courseid,
        |  csc.courseid
        |from sale_course sc join course_shopping_cart csc
        |on sc.courseid=csc.courseid
      """.stripMargin


    sparkSession.sql("use spark_tuning;")
    println("=======================BROADCASTJOIN Hint=============================")
    sparkSession.sql(sqlStr1).explain()
    println("=======================BROADCAST Hint=============================")
    sparkSession.sql(sqlStr2).explain()
    println("=======================MAPJOIN Hint=============================")
    sparkSession.sql(sqlStr3).explain()

    // TODO API的方式
    val sc: DataFrame = sparkSession.sql("select * from sale_course").toDF()
    val csc: DataFrame = sparkSession.sql("select * from course_shopping_cart").toDF()
    println("=======================DF API=============================")
    import org.apache.spark.sql.functions._
    broadcast(sc)
      .join(csc, Seq("courseid"))
      .select("courseid")
      .explain()
    while (true) {

    }
  }

}
