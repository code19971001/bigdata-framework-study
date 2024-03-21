package com.itcode.skew

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Random

object SkewAggregationTuning {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SkewAggregationTuning")
      .set("spark.sql.shuffle.partitions", "36")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    sparkSession.udf.register("random_prefix", (value: Int, num: Int) => randomPrefixUDF(value, num))
    sparkSession.udf.register("remove_random_prefix", (value: String) => removeRandomPrefixUDF(value))


    val sql1: String =
      """
        |select
        |  courseid,
        |  sum(course_sell) totalSell
        |from
        |  (
        |    select
        |      remove_random_prefix(random_courseid) courseid,
        |      course_sell
        |    from
        |      (
        |        select
        |          random_courseid,
        |          sum(sellmoney) course_sell
        |        from
        |          (
        |            select
        |              random_prefix(courseid, 6) random_courseid,
        |              sellmoney
        |            from
        |              spark_tuning.course_shopping_cart where courseid is not null
        |          ) t1
        |        group by random_courseid
        |      ) t2
        |  ) t3
        |group by
        |  courseid
        |order by totalSell
      """.stripMargin


    val sql2: String =
      """
        |select
        |  courseid,
        |  sum(sellmoney) as totalSell
        |from spark_tuning.course_shopping_cart
        |where courseid is not null
        |group by courseid order by totalSell
      """.stripMargin

    sparkSession.sql(sql1).show(10)
    sparkSession.sql(sql2).show(10)

    while (true) {}
  }


  def randomPrefixUDF(value: Int, num: Int): String = {
    new Random().nextInt(num).toString + "_" + value
  }

  def removeRandomPrefixUDF(value: String): String = {
    value.toString.split("_")(1)
  }
}
