package com.itcode.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {

  def main(args: Array[String]): Unit = {
    val sc: SparkConf = new SparkConf().setMaster("local[2]").setAppName("create_rdd")
    val sparkContext: SparkContext = SparkContext.getOrCreate(sc)
//    createRDDByMemory(sparkContext)
//    createRDDByFile(sparkContext)
    testParallelizeByMemory(sparkContext)
    testParallelizeByFile(sparkContext)
    sparkContext.stop()
  }

  /**
   * parallelize or makeRDD. makeRDD在底层实际上是直接调用了parallelize
   */
  private def createRDDByMemory(sc: SparkContext): Unit = {
    val dataSeq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    val sourceRdd1: RDD[Int] = sc.parallelize(dataSeq)
    val sourceRdd2: RDD[Int] = sc.makeRDD(dataSeq)
    println(sourceRdd1.collect().mkString(","))
    sourceRdd2.saveAsTextFile("spark-framework/spark-core/src/main/resources/data/output");
  }

  /**
   * textFile: 以行的方式读取文件，支持文件路径，文件夹，通配符*，分布式文件系统。
   * wholeTextFiles: 以文件的形式读取文件，返回tuple格式的数据。key是文件名，value是文本内容。
   */
  private def createRDDByFile(sc: SparkContext): Unit = {
    val sourceRdd: RDD[String] = sc.textFile("spark-framework/spark-core/src/main/resources/data/wordcount/data.txt")
    val sourceRdd2: RDD[(String, String)] = sc.wholeTextFiles("spark-framework/spark-core/src/main/resources/data/wordcount/data.txt")
    println(sourceRdd.collect().mkString(","))
    println(sourceRdd2.collect().mkString(","))
  }

  private def testParallelizeByMemory(sc: SparkContext): Unit = {
    val dataSeq: Seq[Int] = Seq[Int](1, 2, 3, 4, 5)
    val sourceRdd1: RDD[Int] = sc.parallelize(dataSeq, 3)
    sourceRdd1.saveAsTextFile("spark-framework/spark-core/src/main/resources/data/output");
  }

  private def testParallelizeByFile(sc: SparkContext): Unit = {
    val sourceRdd: RDD[String] = sc.textFile("spark-framework/spark-core/src/main/resources/data/wordcount/testParallelize.txt")
    sourceRdd.saveAsTextFile("spark-framework/spark-core/src/main/resources/data/output");
  }
}
