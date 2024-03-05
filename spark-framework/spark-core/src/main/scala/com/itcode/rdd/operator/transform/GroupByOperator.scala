package com.itcode.rdd.operator.transform

import com.itcode.rdd.utils.SparkUtil

import java.util.Calendar

/**
 * @author : code1997
 * @date : 2021/9/23 23:27
 */
object GroupByOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtil.createSparkContext("GroupByOperator")
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    //区分奇数和偶数:会将分区中的数据打乱，然后重新组合，这个过程我们称之为shuffle
    val groupBy: RDD[(Int, Iterable[Int])] = rdd.groupBy(num => num % 2)
    groupBy.collect().foreach(println)

    //求每个小时的访问量
    val lines: RDD[String] = sc.textFile("spark-framework/spark-core/src/main/resources/data/logs/apache.log")
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = lines.map(line => {
      import java.text.SimpleDateFormat
      import java.util.Date
      val str: Array[String] = line.split(" ")
      val time: Date = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(str(3))
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(time)
      (calendar.get(Calendar.HOUR_OF_DAY).toString, 1)
    }).groupBy(_._1)
    //模式匹配
    timeRDD.map({
      case (hour, iter) =>
        (hour, iter.size)
    }
    ).collect().foreach(println)
    sc.stop()
  }

}
