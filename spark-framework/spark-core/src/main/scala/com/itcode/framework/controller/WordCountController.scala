package com.itcode.framework.controller

import com.itcode.framework.common.TController
import com.itcode.framework.service.WordCountService


/**
 * @author : code1997
 * @date : 2022/3/2 22:10
 */
class WordCountController extends TController {

  private val wordCountService: WordCountService = new WordCountService()

  def dispatch(): Unit = {
    val result: Array[(String, Int)] = wordCountService.dataAnalysis()
    result.foreach(println)
  }

}
