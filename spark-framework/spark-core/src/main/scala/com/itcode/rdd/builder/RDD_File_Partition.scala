package com.itcode.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author : code1997
 * @date : 2021/9/23 20:53
 */
object RDD_File_Partition {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd-file")
    val sc: SparkContext = new SparkContext(sparkConf)
    //第二个参数是最小分区数量，defaultMinPartitions: Int = math.min(defaultParallelism, 2)
    val rdd: RDD[String] = sc.textFile("data/spark-core/wordcount/1.txt", 2)
    //coalesce主要实现重分区，可以用来减少文件的个数.
    rdd.coalesce(1).saveAsTextFile("data/spark-core/output/RDD_File_Partition")
    sc.stop()
  }

}
