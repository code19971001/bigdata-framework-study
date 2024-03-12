package com.itcode.rbo

import com.itcode.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object RboJoinDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("RboJoinDemo").setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    import sparkSession.implicits._
    val peopleDataSet: Dataset[People] = sparkSession.createDataset[People](Seq(People(1, "张三", 101), People(2, "李四", 102), People(3, "王五", 103), People(4, "赵六", 104)))
    val cityDataSet: Dataset[City] = sparkSession.createDataset[City](Seq(City(101,"上海"),City(102,"北京"),City(103,"杭州"),City(104,"洛阳")))
    peopleDataSet.show(10,false)
    cityDataSet.show(10,false)
    peopleDataSet.createOrReplaceTempView("people")
    cityDataSet.createOrReplaceTempView("city")
    val sql1:String = "select * from people p left join city c on p.cityNo=c.cityNo and p.cityNo < 103"
    sparkSession.sql(sql1).show(10,truncate = false)
    sparkSession.sql(sql1).explain(extended = true)
    val sql2: String = "select * from people p left join city c on p.cityNo=c.cityNo where p.cityNo < 103"
    sparkSession.sql(sql2).show(10, truncate = false)
    sparkSession.sql(sql2).explain(extended = true)

  }


}

case class People(idCard: Int, name: String, cityNo: Int)

case class City(cityNo: Int, name: String)