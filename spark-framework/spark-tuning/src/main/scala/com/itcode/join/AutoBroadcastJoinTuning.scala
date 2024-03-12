package com.itcode.join

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AutoBroadcastJoinTuning {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BroadcastJoinTuning")
            .set("spark.sql.autoBroadcastJoinThreshold","-1")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    val sqlStr: String =
      """
        |select
        |  sc.courseid,
        |  csc.courseid
        |from sale_course sc join course_shopping_cart csc
        |on sc.courseid=csc.courseid
      """.stripMargin

    sparkSession.sql("use spark_tuning;")
    sparkSession.sql(sqlStr).show()

    while (true) {}

  }
}
