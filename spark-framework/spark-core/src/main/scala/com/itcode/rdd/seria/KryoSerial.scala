package com.itcode.rdd.seria

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * java的序列化可以序列化任何的类，但是比较重(字节多)，序列化后，对象的提交也比较大。
 * Spark处于性能考虑，Spark2.0开始支持另外一种Kryo序列化机制，Kryo的速度是Serializable的
 * 10倍。当RDD在shuffle数据的时候，简单数据类型，数组和字符串类型已经在Spark内使用kryo进行
 * 序列化。
 *
 * 如果某个字段使用transient代表整个字段不会被序列化，java在设计的时候没有考虑大数据分布式的情况，为了安全考虑
 * java的很多类是使用了transient，比如ArrayList的存放元素数据的数组，可以通过观测文件大小的变化，来发现这个关键字
 * 的作用,kryo序列化绕过了transient的规则,即便是字段被这样标注，也会被序列化。
 *
 *
 * kryo源码：https://github.com/EsotericSoftware/kryo
 * 注：即使使用Kryo序列化，也要继承Serializable接口。
 *
 * @author : code1997
 * @date : 2022/2/19 16:44
 */
object KryoSerial {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Search]))
      .setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    val sourceData: RDD[String] = sc.makeRDD(List("hello world", "hello spark", "it_demo"))
    val search = new Search("h")
    search.getMatch1(sourceData).collect().foreach(println)
    sc.stop()
  }

  class Search(query: String) extends Serializable {


    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    def getMatch1(sourceData: RDD[String]): RDD[String] = {
      sourceData.filter(data => isMatch(data))
    }

    def getMatch2(sourceData: RDD[String]): RDD[String] = {
      val s = query
      sourceData.filter(data => data.contains(s))
      sourceData.filter(data => data.contains(query))
    }

  }

}
