package com.itcode.rdd.operator.transform

/**
 * @author : code1997
 * @date : 2021/9/25 9:30
 */
object SortedByOperator {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sort-by")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("11", 1), ("12", 2), ("2", 3)))
    rdd.sortBy(num=>num._1).collect().foreach(println)
    sc.stop()
  }

}
