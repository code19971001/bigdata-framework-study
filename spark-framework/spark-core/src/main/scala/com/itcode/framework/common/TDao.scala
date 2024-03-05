package com.itcode.framework.common

import com.itcode.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @author : code1997
 * @date : 2022/3/2 23:00
 */
trait TDao {

  def loadDataFromFile(path: String): RDD[String] = {
    EnvUtil.getEnv().textFile(path)
  }
}
