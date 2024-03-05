package com.itcode.framework.util

import org.apache.spark.SparkContext

/**
 * @author : code1997
 * @date : 2022/3/2 23:07
 */
object EnvUtil {

  private val scLocal = new ThreadLocal[SparkContext]()

  def putEnv(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def getEnv(): SparkContext = {
    scLocal.get()
  }

  def clearEnv(): Unit = {
    scLocal.remove()
  }

}
