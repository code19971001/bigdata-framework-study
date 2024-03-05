package com.itcode.rdd.utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {

  def createSparkContext(appName: String): SparkContext = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName)
    SparkContext.getOrCreate(sparkConf)
  }

}
