package com.itcode.rdd.operator.transform

import com.itcode.rdd.utils.SparkUtil

/**
 * 转换操作：map
 *
 * @author : code1997
 * @date : 2021/9/23 21:08
 */
object MapOperatorParallel {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtil.createSparkContext("MapOperatorParallel")
    //如果并行度为1, 那么就全局有序, 都是每一个元素执行完成所有的操作之后才轮到下一个数据.
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 1)
    val map1: RDD[Int] = rdd.map(num => {
      println(">>>>>" + num)
      num
    })
    val map2: RDD[Int] = map1.map(num => {
      println("<<<<<" + num)
      num
    })
    map2.collect()
    sc.stop()

  }

}
