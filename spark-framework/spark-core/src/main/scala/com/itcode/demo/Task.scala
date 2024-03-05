package com.itcode.demo

/**
 * 上层的一个数据结构，封装了数据以及逻辑，当在分布式的情况下需要考虑如何对数据进行划分来实现并行计算，可以类比于RDD。
 *
 * @author : code1997
 * @date : 2022/1/7 23:09
 */
class Task extends Serializable {

  val datas = List(1, 2, 3, 4)

  val logic: (Int) => Int = _ * 2


}
