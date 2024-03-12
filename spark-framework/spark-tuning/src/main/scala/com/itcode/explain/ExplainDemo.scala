package com.itcode.explain

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExplainDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ExplainDemo")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    val sqlStr: String =
      """
        |select
        |  sc.courseid,
        |  sc.coursename,
        |  sum(sellmoney) as total_sell
        |from sale_course sc join course_shopping_cart csc
        |  on sc.courseid=csc.courseid and sc.dt=csc.dt and sc.dn=csc.dn
        |group by sc.courseid,sc.coursename
      """.stripMargin

    sparkSession.sql("use spark_tuning;")

    println("=====================================explain()-只展示物理执行计划============================================")
    sparkSession.sql(sqlStr).explain()

    println("===============================explain(mode = \"simple\")-只展示物理执行计划=================================")
    sparkSession.sql(sqlStr).explain(mode = "simple")

    println("============================explain(mode = \"extended\")-展示逻辑和物理执行计划==============================")
    sparkSession.sql(sqlStr).explain(mode = "extended")

    println("============================explain(mode = \"codegen\")-展示可执行java代码===================================")
    sparkSession.sql(sqlStr).explain(mode = "codegen")

    println("============================explain(mode = \"formatted\")-展示格式化的物理执行计划=============================")
    sparkSession.sql(sqlStr).explain(mode = "formatted")

    println("============================Web UI查看执行计划=============================")
    sparkSession.sql(sqlStr).show()
    Thread.sleep(100000)

  }

}
