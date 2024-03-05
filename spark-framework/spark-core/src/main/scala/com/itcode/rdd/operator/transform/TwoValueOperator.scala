package com.itcode.rdd.operator.transform

/**
 * @author : code1997
 * @date : 2021/9/25 9:42
 */
object TwoValueOperator {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sort-by")
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    //3,4
    val intersection: RDD[Int] = rdd1.intersection(rdd2)
    intersection.collect().foreach(println)
    println("-------------------")
    //1,2,3,4,3,4,5,6不会去除重复的数据
    val union: RDD[Int] = rdd1.union(rdd2)
    union.collect().foreach(println)
    println("-------------------")
    //rdd1中有，rdd2没有的
    val subtract: RDD[Int] = rdd1.subtract(rdd2)
    subtract.collect().foreach(println)
    println("-------------------")
    //返回touple:(1,3),(2,4),(3,5),(4,6)
    val zip: RDD[(Int, Int)] = rdd1.zip(rdd2)
    zip.collect().foreach(println)
    sc.stop()
  }

}
