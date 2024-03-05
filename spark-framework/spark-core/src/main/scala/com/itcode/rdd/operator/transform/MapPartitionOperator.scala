package com.itcode.rdd.operator.transform

import com.itcode.rdd.utils.SparkUtil

/**
 * 转换操作：map
 *
 * @author : code1997
 */
object MapPartitionOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtil.createSparkContext("MapPartitionOperator")
    //分区1:1,2 分区2:3,4
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    //以分区为单位执行
    val map1: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>")
      iter.map(_ * 2)
    })
    //collect是转换操作，用于触发计算
    map1.collect()
    //返回每个分区中的最大值:应该是2和4
    val map2: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    map2.collect().foreach(println)


    //传入的参数包括分区索引，对于不同分区的数据根据index进行不同的处理
    val map3: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        //空的迭代器对象
        Nil.iterator
      }
    })
    map3.collect().foreach(println)

    //分区1的数据为3,4
    val map4: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map((index, _))
    })
    map4.collect().foreach(println)



    //分区0的数据:1,2 分区1的数据:3,4
    val map5: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      println("open database connection for partition " + index)
      val ints: Iterator[Int] = iter.map(data => {
        println("inserting " + data)
        Thread.sleep(10000)
        data
      })
      println(ints.size)
      println("close database connection for partition " + index)
      Thread.sleep(20000000)
      iter
    })
    map5.collect()

    sc.stop()
  }

}
