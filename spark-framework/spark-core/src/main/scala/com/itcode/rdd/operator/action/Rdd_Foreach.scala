package com.itcode.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 思考：什么是算子，为什么被称为算子？
 * RDD的方法被称为算子，和scala集合对象的方法不一样，集合对象的方法否是在同一个节点的内存中完成的，
 * RDD的方法可以将计算逻辑发送到Executor端(分布式节点)执行.为了做出区分，所以将RDD的方法称为算子。
 * 所以RDD的方法外部的操作都是在Driver端操作的，而方法内部的逻辑代码是在Executor端执行的。
 *
 * @author : code1997
 * @date : 2022/2/18 15:27
 */
object Rdd_Foreach {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //collect会将executor中的数据收集到driver端然后进行遍历操作.所以输出是有序的.
    data.collect().foreach(println)
    println("================")
    //直接在executor端进行遍历输出，是分布式打印，所以打印出来的结果就是无序的.
    data.foreach(println)
    //所以如果我们直接使用foreach的话，需要注意里面使用到的对象需要实现序列化接口或者使用样例类(样例类在编译之后会自动拥有序列化特质)，RDD算子中传递的函数会包含闭包操作，那么会进行闭包检测，即在编译期间就会报错。
    val user = new User()
    data.foreach(num => {
      println("age is " + (user.age + num))
    })
    sc.stop()
  }

  class User {
    var age: Int = 29
  }

}
