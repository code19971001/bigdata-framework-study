package com.itcode.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 基于文件创建RDD
 * textFile:一次读取文件中的一行数据.
 * wholeTextFiles：一次读取一个文件，返回值是(文件路径,整个文件内容)
 *
 * @author : code1997
 * @date : 2021/9/22 22:25
 */
object RDD_File {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_File")
    val sc: SparkContext = new SparkContext(sparkConf)
    //textFile不仅仅可以指向具体的文件，而且可仅指向path，也可以使用通配符以及分布式存储系统路径，比如hdfs
    val rdd1: RDD[String] = sc.textFile("data/spark-core/wordcount/1.txt")
    val rdd2: RDD[String] = sc.textFile("E:\\projects\\ideacode\\atguigu\\spark-code1997\\data\\spark-core\\wordcount\\1.txt")
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
    //以文件为单位读取数据，返回是元组，第一个值是文件路径
    val rdd3: RDD[(String, String)] = sc.wholeTextFiles("data/spark-core/wordcount")
    rdd3.collect().foreach(println)
    sc.stop()
  }

}
