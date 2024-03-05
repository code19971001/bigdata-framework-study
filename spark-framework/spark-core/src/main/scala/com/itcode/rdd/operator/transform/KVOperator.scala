package com.itcode.rdd.operator.transform

/**
 * @author : code1997
 * @date : 2021/9/25 9:59
 */
object KVOperator {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.rdd.RDD
    import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("k-v")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRdd:RDD[(Int,Int)] = rdd.map((_,1))
    //存在隐式转换：RDD=>PairRDDFunctions
    //根据指定的分区规则对数据进行重新分区:这里可以选取hash，
    //mapRdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("data/spark-core/output/partitionby")
    println("-----------------------")
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1), ("c", 1)))
    //实际上是两两进行计算，如果只有一个则直接返回
    val reduce: RDD[(String, Int)] = rdd2.reduceByKey((num1, num2) => num1 + num2)
    reduce.collect().foreach(println)
    println("-----------------------")
    val group: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    group.collect().foreach(println)
    


    sc.stop()
  }

}
