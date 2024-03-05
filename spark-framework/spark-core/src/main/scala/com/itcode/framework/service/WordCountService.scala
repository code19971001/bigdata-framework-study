package com.itcode.framework.service

import com.itcode.framework.common.TService
import com.itcode.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author : code1997
 * @date : 2022/3/2 22:10
 */
class WordCountService extends TService {

  private val wordCountDao: WordCountDao = new WordCountDao()


  def dataAnalysis(): Array[(String, Int)] = {
    val lines: RDD[String] = wordCountDao.loadDataFromFile("spark-core/src/main/scala/com/it/framework/words.txt")
    lines.flatMap((_: String).split(" ")).map(((_: String), 1)).reduceByKey((_: Int) + (_: Int)).collect()
  }


}
