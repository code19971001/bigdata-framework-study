package com.itcode.framework.common

import com.itcode.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/3/2 22:45
 */
trait TApplication {

  /**
   * 逻辑抽象
   *
   * @param op
   */
  def start(master: String = "local[*]", app: String = "Application")(op: => scala.Unit): Unit = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sparkContext = new SparkContext(sparkConf)
    EnvUtil.putEnv(sparkContext)
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    sparkContext.stop()
    EnvUtil.clearEnv()
  }


}
