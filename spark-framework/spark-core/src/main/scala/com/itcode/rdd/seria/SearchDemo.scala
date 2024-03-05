package com.itcode.rdd.seria

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 问题：会出现序列化异常：
 * 在scala中类的构造参数是类的属性(private final来修饰的)，
 * 因此实际上调用的时候是Search.来调用,所以会对类进行闭包检测。
 * 解决方案：
 * 1.将类改为样例类。
 * 2.实现序列化。
 * 3.改代码，将query赋值给一个本地变量
 *  before: sourceData.filter(data => data.contains(query))
 *  after:  val s =query
            sourceData.filter(data => data.contains(s))
 *
 * @author : code1997
 * @date : 2022/2/19 15:46
 */
object SearchDemo {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    val sourceData: RDD[String] = sc.makeRDD(List("hello world", "hello spark", "it_demo"))
    val search = new Search("h")
    search.getMatch1(sourceData).collect().foreach(println)
    sc.stop()
  }

  class Search(query: String) {


    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    def getMatch1(sourceData: RDD[String]): RDD[String] = {
      sourceData.filter(data => isMatch(data))
    }

    def getMatch2(sourceData: RDD[String]): RDD[String] = {
      val s =query
      sourceData.filter(data => data.contains(s))
      sourceData.filter(data => data.contains(query))
    }

  }

}
