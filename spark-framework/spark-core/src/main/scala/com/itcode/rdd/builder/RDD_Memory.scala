package com.itcode.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于内存构建RDD
 * 1)parallelize：会使用集群默认的并行度级别。
 * 2)makeRDD:实际上调用的就是parallelize方法，可以指定并行度.
 *
 * @author : code1997
 * @date : 2021/9/22 22:13
 */
object RDD_Memory {

  def main(args: Array[String]): Unit = {
    //local[*]会使用核心数的数量的线程来模拟
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_memory")
    val sc: SparkContext = new SparkContext(sparkConf)
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    //1.使用parallelize创建rdd
    val rdd1: RDD[Int] = sc.parallelize(seq)
    //2.使用makeRdd创建RDD
    val rdd2: RDD[Int] = sc.makeRDD(seq)
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
    val seq2 = Seq[Int](1, 2, 3, 4)
    val rdd3: RDD[Int] = sc.makeRDD(seq2, 2)
    //要求输出目录不存在.
    rdd3.saveAsTextFile("data/spark-core/output/RDD_Memory")
    sc.stop()
  }
}
