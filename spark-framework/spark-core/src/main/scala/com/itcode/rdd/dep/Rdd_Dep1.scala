package com.itcode.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/20 12:17
 */
object Rdd_Dep1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    val sparkContext = new SparkContext(sparkConf)
    val lines: RDD[String] = sparkContext.textFile("data/spark-core/wordcount")
    println(lines.dependencies)
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    println(wordGroup.dependencies)
    val word2Count = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println(word2Count.dependencies)
    val result: Array[(String, Int)] = word2Count.collect()
    result.foreach(println)
    sparkContext.stop()
  }

}
