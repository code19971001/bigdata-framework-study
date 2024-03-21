package com.itcode.dpp

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object DPPDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DPPTest")
      .set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    sparkSession.sql(
      """
        |select a.id,a.name,a.age,b.name
        |from spark_tuning.test_student a
        |inner join spark_tuning.test_school b
        |on a.partition=b.partition and b.id<1000
    """.stripMargin).explain(mode = "extended")
  }

}
