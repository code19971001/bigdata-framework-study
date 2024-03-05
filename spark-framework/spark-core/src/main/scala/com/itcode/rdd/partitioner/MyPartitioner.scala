package com.itcode.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author : code1997
 * @date : 2022/2/22 11:12
 */
object MyPartitioner {
  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    //时间一个分区器，将数据按照key来进行分区
    val data: RDD[(String, String)] = sc.makeRDD(List(("nba", "库里"), ("nba", "詹姆斯"), ("cba", "郭艾伦"), ("wnba", "女篮")))
    val reult: RDD[(String, String)] = data.partitionBy(new ConsumerPartitioner(3))
    reult.saveAsTextFile("data/spark-core/output/consumer_partitioner")
    sc.stop()

  }


  class ConsumerPartitioner(partitions: Int) extends Partitioner {
    //分区的数量
    override def numPartitions: Int = partitions

    //根据key返回数据的分区索引
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }

/*      if (key == "nba") {
        0
      } else if (key == "cba") {
        1
      } else {
        2
      }*/
    }
  }

}
