package cbo

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveStatisticDemo {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("CBOTunning")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    AnalyzeTableAndColumn(sparkSession, "spark_tuning.sale_course", "courseid,dt,dn")
    AnalyzeTableAndColumn(sparkSession, "spark_tuning.course_shopping_cart", "courseid,orderid,dt,dn")
    AnalyzeTableAndColumn(sparkSession, "spark_tuning.course_pay", "orderid,dt,dn")

  }

  def AnalyzeTableAndColumn(sparkSession: SparkSession, tableName: String, columnListStr: String): Unit = {
    //TODO 查看 表级别 信息
    println("=========================================查看" + tableName + "表级别 信息========================================")
    sparkSession.sql("DESC FORMATTED " + tableName).show(100,truncate = false)
    //TODO 统计 表级别 信息
    println("=========================================统计 " + tableName + "表级别 信息========================================")
    sparkSession.sql("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS").show(truncate = false)
    //TODO 再查看 表级别 信息
    println("======================================查看统计后 " + tableName + "表级别 信息======================================")
    sparkSession.sql("DESC FORMATTED " + tableName).show(100,truncate = false)


    //TODO 查看 列级别 信息
    println("=========================================查看 " + tableName + "表的" + columnListStr + "列级别 信息========================================")
    val columns: Array[String] = columnListStr.split(",")
    for (column <- columns) {
      sparkSession.sql("DESC FORMATTED " + tableName + " " + column).show(truncate = false)
    }
    //TODO 统计 列级别 信息
    println("=========================================统计 " + tableName + "表的" + columnListStr + "列级别 信息========================================")
    sparkSession.sql(
      s"""
         |ANALYZE TABLE ${tableName}
         |COMPUTE STATISTICS
         |FOR COLUMNS $columnListStr
    """.stripMargin).show(truncate = false)
    //TODO 再查看 列级别 信息
    println("======================================查看统计后 " + tableName + "表的" + columnListStr + "列级别 信息======================================")
    for (column <- columns) {
      sparkSession.sql("DESC FORMATTED " + tableName + " " + column).show(truncate = false)
    }
  }

}
