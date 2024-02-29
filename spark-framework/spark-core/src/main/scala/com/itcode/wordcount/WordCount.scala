package com.itcode.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    //create spark context
    val conf: SparkConf = new SparkConf().setAppName("word_count").setMaster("local[*]")
    val sparkContext: SparkContext = SparkContext.getOrCreate(conf)

    //transform data
    val source: RDD[String] = sparkContext.textFile("spark-framework/spark-core/src/main/resources/data/wordcount/data.txt")
    //scala自减原则
    val result: RDD[(String, Int)] = source.flatMap(line => line.split(" ")).map(word => Tuple2.apply(word, 1)).reduceByKey(_ + _)
    println(result.collect().mkString(","))
    val result2: RDD[(String, Int)] = source.flatMap(line => line.split(" ")).groupBy(word => word).map {
      case (word, list) =>
        (word, list.size)
    }
    println(result2.collect().mkString(","))

    //stop spark context
    sparkContext.stop()
  }

}
