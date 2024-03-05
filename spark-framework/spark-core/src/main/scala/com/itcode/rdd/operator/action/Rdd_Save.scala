package com.itcode.rdd.operator.action

import org.apache.avro.file.BZip2Codec
import org.apache.curator.framework.imps.GzipCompressionProvider
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.io

/**
 * 保存文件相关的操作：
 * saveAsTextFile:
 * saveAsObjectFile:
 *
 * @author : code1997
 * @date : 2022/2/18 15:12
 */
object Rdd_Save {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val data: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)), 1)
    data.saveAsTextFile("data/spark-core/output/RDD_Save/saveAsTextFile")
    data.saveAsObjectFile("data/spark-core/output/RDD_Save/saveAsObjectFile")
    data.saveAsTextFile("data/spark-core/output/RDD_Save/saveAsObjectFile/gzip", classOf[GzipCodec])
    //只有键值类型才可以使用
    data.saveAsSequenceFile("data/spark-core/output/RDD_Save/saveAsSequenceFile")
    sc.stop()
  }

}
