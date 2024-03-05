package com.itcode.demo

/**
 * @author : code1997
 * @date : 2022/1/7 23:40
 */
class SubTask extends Serializable {

  var datas: List[Int] = _
  var logic: (Int) => Int = _


  //计算
  def compute(): List[Int] = {
    datas.map(logic)
  }


}
