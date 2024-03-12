## Spark 调优
### 1 前提
1.搭建好的hadoop集群，hive，还有spark。

|  | bigdata01 | bigdata02 | bigdata03 |
| --- | --- | --- | --- |
| hadoop | √ | √ | √ |
| hive | √ |  |  |
| spark | √(调度为yarn) |  |  |

2.准备测试数据
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173023364-e4e0fd0d-a6a5-49be-bcad-c079840ca58d.png#averageHue=%23fbfbfa&clientId=u8bea0fba-815c-4&from=paste&id=uc3849e33&originHeight=372&originWidth=1460&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u64e2f938-1857-4613-ac4f-f19221895b8&title=)
3.导入数据到hive表中
```scala
object InitUtil {

  def main(args: Array[String]): Unit = {
    //主要用于kerberos的认证
    try {

      //等同于把krb5.conf放在$JAVA_HOME\jre\lib\security，一般写代码即可
      System.setProperty("java.security.krb5.conf", "C:\\ProgramData\\MIT\\Kerberos5\\krb5.ini")
      //下面的conf可以注释掉是因为在core-site.xml里有相关的配置，如果没有相关的配置，则下面的代码是必须的
      //      val conf = new Configuration
      //      conf.set("hadoop.security.authentication", "kerberos")
      //      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("code1997@CODE1997.COM", "C:\\ProgramData\\MIT\\Kerberos5\\code1997.keytab")
      println(UserGroupInformation.getCurrentUser, UserGroupInformation.getLoginUser)
    } catch {
      case e: Exception =>
      e.printStackTrace()
    }
    val sparkConf = new SparkConf().setAppName("InitData")
    .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = initSparkSession(sparkConf)
    initHiveTable(sparkSession)
    //initBucketTable(sparkSession)
    saveData(sparkSession)
  }

  def initSparkSession(sparkConf: SparkConf): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "code1997")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // TODO 改成自己的hadoop的nameNode的url，在core-site文件中
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop02:8020")
    sparkSession
  }

  def initHiveTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/origin_data/sparktuning/coursepay.log")
    .write.partitionBy("dt", "dn")
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .saveAsTable("sparktuning.course_pay")

    sparkSession.read.json("/origin_data/sparktuning/salecourse.log")
    .write.partitionBy("dt", "dn")
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .saveAsTable("sparktuning.sale_course")

    sparkSession.read.json("/origin_data/sparktuning/courseshoppingcart.log")
    .write.partitionBy("dt", "dn")
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .saveAsTable("sparktuning.course_shopping_cart")

  }

  def initBucketTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/origin_data/sparktuning/coursepay.log")
    .write.partitionBy("dt", "dn")
    .format("parquet")
    .bucketBy(5, "orderid")
    .sortBy("orderid")
    .mode(SaveMode.Overwrite)
    .saveAsTable("sparktuning.course_pay_cluster")
    sparkSession.read.json("/origin_data/sparktuning/courseshoppingcart.log")
    .write.partitionBy("dt", "dn")
    .bucketBy(5, "orderid")
    .format("parquet")
    .sortBy("orderid")
    .mode(SaveMode.Overwrite)
    .saveAsTable("sparktuning.course_shopping_cart_cluster")
  }

  def saveData(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => Student(item, "name" + item, random.nextInt(100), random.nextInt(100)))
    }).write.partitionBy("partition")
    .mode(SaveMode.Append)
    .saveAsTable("sparktuning.test_student")

    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => School(item, "school" + item, random.nextInt(100)))
    }).write.partitionBy("partition")
    .mode(SaveMode.Append)
    .saveAsTable("sparktuning.test_school")
  }
}
```
4.使用spark-submit提交任务
```shell
bin/spark-submit --master yarn --deploy-mode client --driver-memory 2g \
--num-executors 3 --executor-cores 2 --executor-memory 3g \ 
--class com.itcode.utils.InitUtil \
/opt/app/spark_tuning/spark-tuning-1.0-SNAPSHOT-jar-with-dependencies.jar
```
注：找不到JDO相关的jar
![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1710221785208-9a6a3b87-997a-43dd-b3fe-917060a2bfab.png#averageHue=%23161516&clientId=u62875e1f-9554-4&from=paste&height=302&id=u2158a2ba&originHeight=453&originWidth=2654&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=155103&status=done&style=none&taskId=u98de7505-b0b5-4a7e-a8a1-c6549e94264&title=&width=1769.3333333333333)
hive的元数据存储到了mysql中，spark找不到对应的jar，将JDO相关的jar复制到SPARK_HOME/jars下即可

- datanucleus-api-jdo-4.2.4.jar
- datanucleus-core-4.1.17.jar
- datanucleus-rdbms-4.1.19.jar
- mysql-connector-j-8.0.31.jar
### 2 explain
_explain可以用来查看spark sql的执行计划。_
#### 2.1 基本语法
.explain(mode='xxx')

- mode="simple"：只展示物理执行计划。
- mode="extended"：展示物理执行计划和逻辑执行计划。
- mode="codegen"：展示要 Codegen 生成的可执行 Java 代码。
- mode="cost"：展示优化后的逻辑执行计划以及相关的统计。
- mode="formatted"：以分隔的方式输出，它会输出更易读的物理执行计划，并展示每个节点的详细信息。
#### 2.2 执行计划处理流程
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173023602-dfdff56a-37f0-4a1b-90a8-21e8637f989c.png#averageHue=%23f7f6f3&clientId=u8bea0fba-815c-4&from=paste&id=u17f741b5&originHeight=345&originWidth=769&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u84476de0-7436-4709-b6ab-e7c4d78adc8&title=)
```scala
object ExplainDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ExplainDemo")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    val sqlStr: String =
      """
        |select
        |  sc.courseid,
        |  sc.coursename,
        |  sum(sellmoney) as total_sell
        |from sale_course sc join course_shopping_cart csc
        |  on sc.courseid=csc.courseid and sc.dt=csc.dt and sc.dn=csc.dn
        |group by sc.courseid,sc.coursename
      """.stripMargin

    sparkSession.sql("use spark_tuning;")

    println("=====================================explain()-只展示物理执行计划============================================")
    sparkSession.sql(sqlStr).explain()

    println("===============================explain(mode = \"simple\")-只展示物理执行计划=================================")
    sparkSession.sql(sqlStr).explain(mode = "simple")

    println("============================explain(mode = \"extended\")-展示逻辑和物理执行计划==============================")
    sparkSession.sql(sqlStr).explain(mode = "extended")

    println("============================explain(mode = \"codegen\")-展示可执行java代码===================================")
    sparkSession.sql(sqlStr).explain(mode = "codegen")

    println("============================explain(mode = \"formatted\")-展示格式化的物理执行计划=============================")
    sparkSession.sql(sqlStr).explain(mode = "formatted")

  }

}
```
执行计划的展示：
```
============================explain(mode = "extended")-展示逻辑和物理执行计划==============================
== Parsed Logical Plan ==
'Aggregate ['sc.courseid, 'sc.coursename], ['sc.courseid, 'sc.coursename, 'sum('sellmoney) AS total_sell#41]
+- 'Join Inner, ((('sc.courseid = 'csc.courseid) AND ('sc.dt = 'csc.dt)) AND ('sc.dn = 'csc.dn))
   :- 'SubqueryAlias sc
   :  +- 'UnresolvedRelation [sale_course], [], false
   +- 'SubqueryAlias csc
      +- 'UnresolvedRelation [course_shopping_cart], [], false

== Analyzed Logical Plan ==
courseid: bigint, coursename: string, total_sell: double
Aggregate [courseid#3L, coursename#5], [courseid#3L, coursename#5, sum(cast(sellmoney#23 as double)) AS total_sell#41]
+- Join Inner, (((courseid#3L = courseid#18L) AND (dt#15 = dt#24)) AND (dn#16 = dn#25))
   :- SubqueryAlias sc
   :  +- SubqueryAlias spark_catalog.spark_tuning.sale_course
   :     +- Relation spark_tuning.sale_course[chapterid#1L,chaptername#2,courseid#3L,coursemanager#4,coursename#5,edusubjectid#6L,edusubjectname#7,majorid#8L,majorname#9,money#10,pointlistid#11L,status#12,teacherid#13L,teachername#14,dt#15,dn#16] parquet
   +- SubqueryAlias csc
      +- SubqueryAlias spark_catalog.spark_tuning.course_shopping_cart
         +- Relation spark_tuning.course_shopping_cart[_corrupt_record#17,courseid#18L,coursename#19,createtime#20,discount#21,orderid#22,sellmoney#23,dt#24,dn#25] parquet

== Optimized Logical Plan ==
Aggregate [courseid#3L, coursename#5], [courseid#3L, coursename#5, sum(cast(sellmoney#23 as double)) AS total_sell#41]
+- Project [courseid#3L, coursename#5, sellmoney#23]
   +- Join Inner, (((courseid#3L = courseid#18L) AND (dt#15 = dt#24)) AND (dn#16 = dn#25))
      :- Project [courseid#3L, coursename#5, dt#15, dn#16]
      :  +- Filter ((isnotnull(courseid#3L) AND isnotnull(dt#15)) AND isnotnull(dn#16))
      :     +- Relation spark_tuning.sale_course[chapterid#1L,chaptername#2,courseid#3L,coursemanager#4,coursename#5,edusubjectid#6L,edusubjectname#7,majorid#8L,majorname#9,money#10,pointlistid#11L,status#12,teacherid#13L,teachername#14,dt#15,dn#16] parquet
      +- Project [courseid#18L, sellmoney#23, dt#24, dn#25]
         +- Filter ((isnotnull(courseid#18L) AND isnotnull(dt#24)) AND isnotnull(dn#25))
            +- Relation spark_tuning.course_shopping_cart[_corrupt_record#17,courseid#18L,coursename#19,createtime#20,discount#21,orderid#22,sellmoney#23,dt#24,dn#25] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[courseid#3L, coursename#5], functions=[sum(cast(sellmoney#23 as double))], output=[courseid#3L, coursename#5, total_sell#41])
   +- Exchange hashpartitioning(courseid#3L, coursename#5, 200), ENSURE_REQUIREMENTS, [plan_id=127]
      +- HashAggregate(keys=[courseid#3L, coursename#5], functions=[partial_sum(cast(sellmoney#23 as double))], output=[courseid#3L, coursename#5, sum#47])
         +- Project [courseid#3L, coursename#5, sellmoney#23]
            +- BroadcastHashJoin [courseid#3L, dt#15, dn#16], [courseid#18L, dt#24, dn#25], Inner, BuildLeft, false
               :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false], input[2, string, true], input[3, string, true]),false), [plan_id=122]
               :  +- Filter isnotnull(courseid#3L)
               :     +- FileScan parquet spark_tuning.sale_course[courseid#3L,coursename#5,dt#15,dn#16] Batched: true, DataFilters: [isnotnull(courseid#3L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[hdfs://bigdata01:9000/user/hive/warehouse/spark_tuning.db/sale_course/..., PartitionFilters: [isnotnull(dt#15), isnotnull(dn#16)], PushedFilters: [IsNotNull(courseid)], ReadSchema: struct<courseid:bigint,coursename:string>
               +- Filter isnotnull(courseid#18L)
                  +- FileScan parquet spark_tuning.course_shopping_cart[courseid#18L,sellmoney#23,dt#24,dn#25] Batched: true, DataFilters: [isnotnull(courseid#18L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[hdfs://bigdata01:9000/user/hive/warehouse/spark_tuning.db/course_shopp..., PartitionFilters: [isnotnull(dt#24), isnotnull(dn#25)], PushedFilters: [IsNotNull(courseid)], ReadSchema: struct<courseid:bigint,sellmoney:string>

```

1. Unresolved 逻辑执行计划：== Parsed Logical Plan ==
   - Parser 组件检查 SQL 语法上是否有问题，然后生成 Unresolved（未决断）的逻辑计划，不检查表名、不检查列名。
2. Resolved 逻辑执行计划：== Analyzed Logical Plan ==
   - 通过访问 Spark 中的 Catalog 存储库来解析验证语义、列名、类型、表名等。
3. 优化后的逻辑执行计划：== Optimized Logical Plan ==
   - Catalyst 优化器根据各种规则进行优化。RBO(rules base Optimized)
4. 物理执行计划：== Physical Plan ==
   - HashAggregate 运算符表示数据聚合，一般 HashAggregate 是成对出现，第一个HashAggregate 是将执行节点本地的数据进行局部聚合，另一个 HashAggregate 是将各个分区的数据进一步进行聚合计算。
   - Exchange 运算符其实就是 shuffle，表示需要在集群上移动数据。很多时候HashAggregate 会以 Exchange 分隔开来。
   - Project 运算符是 SQL 中的投影操作，就是选择列(列裁剪)（例如：select name, age…）。
   - BroadcastHashJoin 运算符表示通过基于广播方式进行 HashJoin，一般用于大小表join，将小表广播。
   - LocalTableScan 运算符就是全表扫描本地的表。

web ui方式：实际工作过程中，我们一般通过web ui来查看执行计划。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173023381-b63e9d81-e433-4711-ab69-647d487c87d3.png#averageHue=%23fbfbfa&clientId=u8bea0fba-815c-4&from=paste&id=u1af8c110&originHeight=785&originWidth=1639&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=ub8dace20-ad29-410d-a48c-9acf9187cb9&title=)
### 3 资源调优
#### 3.1 服务器资源
##### 3.1.1 总体考虑
以单台服务器 128G 内存，32 线程为例。先设定单个 Executor 核数，根据 Yarn 配置得出每个节点最多的 Executor 数量，每个节点的 yarn支配的内存/每个节点数量=单个节点的executor数量。那么总的 executor 数=单节点数量*节点数。
2.具体的提交参数
1）executor-cores：每个 executor 的最大核数。根据经验实践，设定在 3~6 之间比较合理。
2）num-executors：spark job总的executor的数量
考虑到系统基础服务和 HDFS 等组件的余量，yarn.nodemanager.resource.cpu-vcores 配置为：28，参考executor-cores 的值为：4，那么每个 node 的 executor 数 = 28/4 = 7,假设集群节点为 10，那么 num-executors = 7 * 10 = 70
3）executor-memory：粗略估算该参数值=yarn-nodemanager.resource.memory-mb / 每个节点的 executor 数量。如果 yarn 的参数配置为 100G，那么每个 Executor 大概就是 100G/7≈14G，同时要注意yarn 配置中每个容器允许的最大内存是否匹配。
3.实际

|  | bigdata01 | bigdata02 | bigdata03 |
| --- | --- | --- | --- |
| vcore | 4 | 4 | 4 |
| memory | 16 | 12 | 12 |

##### 3.1.2 内存估算
spark内存模型：
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173023424-1265ddcb-022c-45bb-b7c8-4cc00115b48a.png#averageHue=%23afad4d&clientId=u8bea0fba-815c-4&from=paste&id=u417158b4&originHeight=386&originWidth=690&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u12b3b1ed-bb80-4055-946c-d2b8edfbabb&title=)

- Storage内存=广播变量(每一个executor都会缓存一份)+cache/executor数量。
- Executor内存=每个Excutor核数*(数据集大小/并行度)，比如reduceByKey,GroupByKey中的shuffle，对于sparksql来说，默认的shuffle的并行度为200。
- Other内存=自定义数据结构*每个Executor核数。
##### 2.1.3 调整内存配置项
一般情况下，各个区域的内存比例保持默认值即可。
spark.memory.fraction=（估算 storage 内存+估算 Execution 内存）/（估算 storage 内存+估算 Execution 内存+估算 Other 内存），一般是60%
spark.memory.storageFraction =（估算 storage 内存）/（估算 storage 内存+估算Execution 内存）
代入公式：
Storage 堆内内存=(spark.executor.memory–300MB)_spark.memory.fraction_spark.memory.storageFraction，如果不够会落盘。
Execution 堆内内存=(spark.executor.memory–300MB)_spark.memory.fraction_(1-spark.memory.storageFraction)，如果不够会直接挂掉。
#### 3.2 Cache和序列化
##### 3.2.1 RDD
>对于RDD操作，我们需要注意对象的序列化方式，kryo会有比较大的增益，对于dataset而言，基本上不需要我们设置序列化方式。
1.cache
默认的情况下是基于内存的，使用JVM的默认的序列化方式。
```scala
/**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   */
def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

/**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   */
def cache(): this.type = persist()
```
```scala
object RddCacheDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("RddCacheDemo")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    val result: RDD[Row] = sparkSession.sql("select * from spark_tuning.course_pay").rdd
    result.cache()
    result.foreachPartition((p: Iterator[Row]) => p.foreach(item => println(item.get(0))))
    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让spark context立马结束
    }
  }

}
```
```scala
/**
 * Various [[org.apache.spark.storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val DISK_ONLY_3 = new StorageLevel(true, false, false, false, 3)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(true, true, true, false, 1)
```
提交代码：需要注意自己的虚拟机的参数信息
```shell
./spark-submit --master yarn --deploy-mode client \
> --driver-memory 1g --num-executors 3 --executor-cores 2 --executor-memory 3g \
> --class com.itcode.cache.RddCacheDemo /opt/app/spark_tuning/spark-tuning-1.0-SNAPSHOT-jar-with-dependencies.jar
```
我这里使用本地去run
![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1710229886264-073d53a4-9a42-49f4-8821-9bb293ffc80b.png#averageHue=%23faf9f9&clientId=u62875e1f-9554-4&from=paste&height=234&id=EB8bE&originHeight=351&originWidth=1672&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=61995&status=done&style=none&taskId=ua7ca3131-a415-4405-a8a0-10d89e4e47f&title=&width=1114.6666666666667)
2.cache with kyro ser
demo code:
```scala
def main(args: Array[String]): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("RddCacheKryoDemo")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[CoursePay]))


  val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

  import sparkSession.implicits._
  val result = sparkSession.sql("select * from sparktuning.course_pay").as[CoursePay].rdd
  result.persist(StorageLevel.MEMORY_ONLY_SER)
  result.foreachPartition(( p: Iterator[CoursePay] ) => p.foreach(item => println(item.orderid)))

  while (true) {
    //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让sparkcontext立马结束
  }
}
```
效果对比![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1710232121408-6354743f-3e07-442c-b4f0-c7ebb0596b83.png#averageHue=%23faf9f9&clientId=u62875e1f-9554-4&from=paste&height=235&id=u3f06fd8e&originHeight=353&originWidth=1714&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=64003&status=done&style=none&taskId=u7a6a7188-f16b-4220-9a54-0981865cdee&title=&width=1142.6666666666667)
##### 3.2.2 Dataset
默认的缓存级别是**内存和磁盘**，如果内存不够的情况下会缓存到磁盘中。
```scala
def cacheQuery(
  query: Dataset[_],
  tableName: Option[String] = None,
  storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = 


/**
 * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
 *
 * @group basic
 * @since 1.6.0
 */
def persist(): this.type = {
  sparkSession.sharedState.cacheManager.cacheQuery(this)
  this
}

/**
 * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
 *
 * @group basic
 * @since 1.6.0
 */
def cache(): this.type = persist()
```
```
def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("DataSetCacheDemo")
//      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    import sparkSession.implicits._
    val result = sparkSession.sql("select * from sparktuning.course_pay").as[CoursePay]
    result.cache()
    result.foreachPartition(( p: Iterator[CoursePay] ) => p.foreach(item => println(item.orderid)))
    while (true) {
    }

  }
```
效果图：上为MEMORY_AND_DISK，下为MEMORY_AND_DISK_SER。
spark sql而言存在encoder，实现了自己的序列化方式，因此不需要我们自己来注册序列化方式。
![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1710231858868-674a9f8f-abb6-4027-9aba-162ff78d4a72.png#averageHue=%23f7f7f7&clientId=u62875e1f-9554-4&from=paste&height=334&id=m1Ca2&originHeight=501&originWidth=2073&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=112344&status=done&style=none&taskId=ued32cf9c-bd3b-48bc-a2f3-8d5818adffd&title=&width=1382)
#### 3.3 CPU调优
##### 3.3.1 并行度和并发度
1）并行度

- spark.default.parallelism：设置 RDD 的默认并行度，没有设置时，由 join、reduceByKey 和 parallelize 等转换决定，不能设置spark sql的数据结构的并行度。
- spark.sql.shuffle.partitions：
- 适用 SparkSQL 时，Shuffle Reduce 阶段默认的并行度，默认 200。此参数只能控制Spark sql、DataFrame、DataSet 分区个数。不能控制 RDD 分区个数

2）并发度
同时执行的task数，取决于executor的
##### 3.3.2 cpu低效的原因
Executor 接收到 TaskDescription 之后，首先需要对 TaskDescription 反序列化才能读取任务信息，然后将任务代码再反序列化得到可执行代码，最后再结合其他任务信息创建TaskRunner。当数据过于分散，分布式任务数量会大幅增加，但每个任务需要处理的数据量却少之又少，就 CPU 消耗来说，相比花在数据处理上的比例，任务调度上的开销几乎与之分庭抗礼。显然，在这种情况下，CPU 的有效利用率也是极低的。
1.并行度较低
并行度较低，数据分片较大，容易导致cpu线程挂起(其他资源不如，比如内存)。如果每个executor存在4个vcore，内存10g，每个task需要跑5g内存，那么实际上会有2个vcore被浪费掉。
2.并行度过高
数据过于分散，数据分片太少，task调度开销更多。
3.如何设置比较合适？
为了合理利用资源，**一般会将并行度（task 数）设置成并发度（vcore 数）的 2 倍到 3 倍**。假设我们当前任务的提交参数中是12个vcode，那么将这个参数设置为24~36是比较合适的。
### 4 spark sql优化
_SparkSQL 在整个执行计划处理的过程中，使用了 Catalyst 优化器。_
#### 4.1 RBO优化
_RBO：rule base Optimiztion，基于规则的优化，主要用于对逻辑执行计划的优化。主要发生在Analyzed Logical Plan => Optimized Logical Plan阶段。_
##### 4.1.1 谓词下推(Predicate Pushdown)
谓词指的是那些过滤逻辑，谓词下推就是尽可能将这些逻辑提前执行，减少下游处理的数据量，而这些规则对于Parquet，ORC这类存储格式，结合文件注脚中的统计信息，可以大幅度的减少数据扫描量，降低磁盘的I/O开销。
eg：左表left join右表

|  | 针对左表的filter | 针对右表的filter |
| --- | --- | --- |
| Join 中条件（on ） | 只下推右表 | 只下推右表 |
| Join 后条件（where ） | 两表都下推 | 两表都下推 |

- 因为**默认对于下推的谓词存在非空判断**，因此可能会造成筛选条件放到on或者where后面的**执行结果**有所不同。
- 对于inner join，无论是写到on还是where后面都会自动进行优化。
##### 4.1.2 列裁剪
扫描数据源的时候，只读取那些与查询相关的字段，对于列式存储格式来说可以显著的减少数据量的读取。
##### 4.1.3 常量替换
如果我们在select语句中参杂一些常量表达式，那么Catalyst也会自动用表达式的结果进行替换。
#### 4.2 CBO优化
##### 4.2.1 什么是CBO?
CBO（_cost base Optimiztion_） 优化主要在物理计划层面，原理是计算所有可能的物理计划的代价，并挑选出代价最小的物理执行计划，充分考虑出数据本身的特点(大小和分布)以及操作算子的代价，从而选择执行代价最小的物理执行计划。
而每个执行节点的代价，分为两个部分:

1. 该执行节点对数据集的影响，即该节点输出数据集的大小与分布
   1. 初始数据集，也即原始表，其数据集的大小与分布可直接通过统计得到。
   2. 中间节点输出数据集的大小与分布可由其输入数据集的信息与操作本身的特点推算。
2. 该执行节点操作算子的代价：代价相对固定，可以用规则来描述。
##### 4.2.2 Statistics收集
_需要先执行特定的 SQL 语句来收集所需的表和列的统计信息。_
1.表级别的统计信息(扫表)
生成 表大小sizeInBytes 和 表rowCount。
```sql
#无法计算非hdfs数据源的表的文件大小
ANALYZE TABLE 表名 COMPUTE STATISTICS
```
2.表级别的统计信息(不扫描)
只生成 sizeInBytes，如果原来已经生成过 sizeInBytes 和 rowCount，而本次生成的sizeInBytes 和原来的大小一样，则保留 rowCount（若存在），否则清除 rowCount。
```sql
ANALYZE TABLE 表名 COMPUTE STATISTICS NOSCAN
```
3.生成列级别的信息
ANALYZE TABLE 表名 COMPUTE STATISTICS FOR COLUMNS 列 1,列 2,列 3
生成列统计信息，为保证一致性，会同步更新表统计信息。目前不支持复杂数据类型（如 Seq, Map 等）和 HiveStringType 的统计信息生成。
4.显示统计信息
```sql
DESC FORMATTED 表名
DESC FORMATTED 表名 列名
```
##### 4.2.3 使用CBO
通过spark.sql.cbo.enabled来开启，默认是 false。配置开启 CBO 后，CBO 优化器可以基于表和列的统计信息，进行一系列的估算，最终选择出最优的查询计划。比如：Build 侧择优化、优化 Join 类型、优化多表 Join 顺序等。如果cbo开启的情况下，可能存在修改join的类型

| 参数 | 描述 | 默认值 |
| --- | --- | --- |
| spark.sql.cbo.enabled | CBO 总开关。true 表示打开，false 表示关闭。要使用该功能，需确保相关表和列的统计信息已经生成。 | false |
| spark.sql.cbo.joinReorder.enabled | 使用 CBO 来自动调整连续的 inner join 的顺序。true：表示打开，false：表示关闭。要使用该功能，需确保相关表和列的统计信息已经生成，且CBO 总开关打开。 | false |
| spark.sql.cbo.joinReorder.dp.threshold | 使用 CBO 来自动调整连续 inner join 的表的个数阈值。如果超出该阈值，则不会调整 join 顺序。 | 12 |

#### 4.3 join
##### 4.3.1 广播join
Spark join 策略中，如果当一张小表足够小并且可以先缓存到内存中，那么可以使用Broadcast Hash Join，其原理就是先将小表聚合到 driver 端，再广播到各个大表分区中(实际上是以executor为单位的)，每个executor共享小表，那么再次进行 join 的时候，就相当于大表的各自分区的数据与小表进行本地 join，从而规避了shuffle。
> 广播 join 默认值为 10MB，由 spark.sql.autoBroadcastJoinThreshold 参数控制。但是我们可以强制实现广播。

```scala
object AutoBroadcastJoinTuning {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BroadcastJoinTuning")
            .set("spark.sql.autoBroadcastJoinThreshold","10m")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    val sqlStr: String =
      """
        |select
        |  sc.courseid,
        |  csc.courseid
        |from sale_course sc join course_shopping_cart csc
        |on sc.courseid=csc.courseid
      """.stripMargin

    sparkSession.sql("use spark_tuning;")
    sparkSession.sql(sqlStr).show()

    while (true) {}

  }
}
```
关闭boardcast join
![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1710255390800-b74f5ed9-f4d3-426d-afe5-05f17e71fda4.png#averageHue=%23f9f9f8&clientId=u62875e1f-9554-4&from=paste&height=311&id=u785ba00e&originHeight=467&originWidth=1881&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=99640&status=done&style=none&taskId=ucd97c301-d429-42bc-9f54-3cfd2684b9c&title=&width=1254)
使用boardcast join
![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1710255341335-3075b11b-99ad-49dc-87eb-134a81ed566d.png#averageHue=%23fafaf9&clientId=u62875e1f-9554-4&from=paste&height=240&id=uf8998de9&originHeight=360&originWidth=1890&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=79369&status=done&style=none&taskId=u246fbd5f-1226-4ca5-b928-566430acd8f&title=&width=1260)
##### 4.3.2 SMB join
SMB JOIN 是 sort merge bucket 操作，需要进行分桶，首先会进行排序，然后根据 key值合并，把相同 key 的数据放到同一个 bucket 中（按照 key 进行 hash）。分桶的目的其实就是把大表化成小表。相同 key 的数据都在同一个桶中之后，再进行 join 操作，那么在联合的时候就会大幅度的减小无关项的扫描。
使用要求：

- 两表进行分桶，桶的个数必须相等。
- 两边进行join的时候，join列=排序列=分桶列
```
def main( args: Array[String] ): Unit = {

  val sparkConf = new SparkConf().setAppName("SMBJoinTuning")
    .set("spark.sql.shuffle.partitions", "36")
  val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
  useSMBJoin(sparkSession)

}

def useSMBJoin( sparkSession: SparkSession ) = {
  //查询出三张表 并进行join 插入到最终表中
  val saleCourse = sparkSession.sql("select *from sparktuning.sale_course")
  val coursePay = sparkSession.sql("select * from sparktuning.course_pay_cluster")
    .withColumnRenamed("discount", "pay_discount")
    .withColumnRenamed("createtime", "pay_createtime")
  val courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart_cluster")
    .drop("coursename")
    .withColumnRenamed("discount", "cart_discount")
    .withColumnRenamed("createtime", "cart_createtime")

  val tmpdata = courseShoppingCart.join(coursePay, Seq("orderid"), "left")
  val result = broadcast(saleCourse).join(tmpdata, Seq("courseid"), "right")
  result
    .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
      , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
      "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "sparktuning.sale_course.dt", "sparktuning.sale_course.dn")
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable("sparktuning.salecourse_detail_2")

}
```
### 5 数据倾斜
#### 5.1 数据倾斜的现象
大多数的task的执行速度很快，但是存在几个task任务运行及其缓慢，甚至于慢慢的出现内存溢出的现象。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173024436-16977060-fce8-4022-8b77-c7e4dd52b053.png#averageHue=%23fdf9f7&clientId=u8bea0fba-815c-4&from=paste&id=u16e35ea0&originHeight=331&originWidth=721&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u1d07aa48-f379-4910-b595-672815b03d6&title=)
原因：
一般来说发生在shuffle类的算子，比如distinct，groupByKey，ReduceByKey，join等。涉及到数据的重分区，如果其中某一个key数量特别大，就发生了数据倾斜。
#### 5.2 大key定位
策略：从所有 key 中，把其中每一个 key 随机取出来一部分，然后进行一个百分比的推算，这是用局部取推算整体，虽然有点不准确，但是在整体概率上来说，我们只需要大概就可以定位那个最多的 key了。
```
def main( args: Array[String] ): Unit = {

  val sparkConf = new SparkConf().setAppName("BigJoinDemo")
    .set("spark.sql.shuffle.partitions", "36")
    .setMaster("local[*]")
  val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

  println("=============================================csc courseid sample=============================================")
  val cscTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_shopping_cart","courseid")
  println(cscTopKey.mkString("\n"))

  println("=============================================sc courseid sample=============================================")
  val scTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.sale_course","courseid")
  println(scTopKey.mkString("\n"))

  println("=============================================cp orderid sample=============================================")
  val cpTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_pay","orderid")
  println(cpTopKey.mkString("\n"))

  println("=============================================csc orderid sample=============================================")
  val cscTopOrderKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_shopping_cart","orderid")
  println(cscTopOrderKey.mkString("\n"))
}


def sampleTopKey( sparkSession: SparkSession, tableName: String, keyColumn: String ): Array[(Int, Row)] = {
  val df: DataFrame = sparkSession.sql("select " + keyColumn + " from " + tableName)
  val top10Key = df
    .select(keyColumn).sample(false, 0.1).rdd // 对key不放回采样
    .map(k => (k, 1)).reduceByKey(_ + _) // 统计不同key出现的次数
    .map(k => (k._2, k._1)).sortByKey(false) // 统计的key进行排序
    .take(10)
  top10Key
}
```
#### 5.3 倾斜优化
##### 5.3.1 单表数据倾斜优化
为了减少shuffle数据量以及reduce端的压力，Spark sql通常是预聚合+exchange+reduce端聚合，所以执行计划中HashAggregate通常是成对出现的。
解决方式：两阶段聚合(加盐局部聚合，去盐全局聚合)。
##### 5.3.2 Join优化
1）广播优化
适用于小表 join 大表。小表足够小，可被加载进 Driver 并通过 Broadcast 方法广播到各个 Executor 中，可以直接规避掉此shuffle阶段，直接优化掉stage，而且广播join也是SparkSql中最常用的优化方案。
2）拆分大key打散小表
解决逻辑

1. 将存在倾斜的表，根据抽样结果，拆分为倾斜 key（skew 表）和没有倾斜 key（common）的两个数据集。
2. 将 skew 表的 key 全部加上随机前缀，然后对另外一个不存在严重数据倾斜的数据集（old 表）整体与随机前缀集作笛卡尔乘积（即将数据量扩大 N 倍，得到 new 表）。
3. 打散的 skew 表 join 扩容的 new 表 union Common 表 join old 表

实现思路：

1. 打散大表：实际就是数据一进一出进行处理，对大 key 前拼上随机前缀实现打散。
2. 扩容小表：实际就是将 DataFrame 中每一条数据，转成一个集合，并往这个集合里循环添加 10 条数据，最后使用 flatmap 压平此集合，达到扩容的效果.
### 6 Job优化
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173024440-17736800-acd2-442f-a65c-d08c23caf4c5.png#averageHue=%23d8e8b7&clientId=u8bea0fba-815c-4&from=paste&id=u41bf3b7b&originHeight=409&originWidth=713&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=uf44a1aa2-068d-4e61-af4a-d4ad6e4bad8&title=)
#### 6.1 Map端优化
##### 6.1.1 Map 端聚合
SparkSQL 本身的 HashAggregte 就会实现本地预聚合+全局聚合。
##### 6.1.2 读取小文件优化
读取的数据源有很多小文件，会造成查询性能的损耗，大量的数据分片信息以及对应产生的 Task 元信息也会给 Spark Driver 的内存造成压力，带来单点问题。
参数：

- spark.sql.files.maxPartitionBytes=128MB 默认 128m：文件最大分区字节数。
- spark.files.openCostInBytes=4194304 默认 4m：打开一个文件的开销。

![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173024559-dbac55ca-6f2d-4071-afff-3ed84a06b075.png#averageHue=%23fcf9f8&clientId=u8bea0fba-815c-4&from=paste&id=u2ca05448&originHeight=746&originWidth=777&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u1ae0c5dc-c0d7-4b3d-bf85-cf9901cc240&title=)
解析：

1. 切片大小= Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))，计算 totalBytes 的时候，每个文件都要加上一个 open 开销defaultParallelism 就是 RDD 的并行度。
2. 当（文件 1 大小+ openCostInBytes）+（文件 2 大小+ openCostInBytes）+…+（文件n-1 大小+ openCostInBytes）+ 文件 n <= maxPartitionBytes 时，n 个文件可以读入同一个分区，即满足： N 个小文件总大小 + （N-1）*openCostInBytes <= maxPartitionBytes 的话。6.2.
##### 6.1.3 增大map 溢写 时流 输出流 buffer
1）map 端 Shuffle Write 有一个缓冲区，初始阈值 5m，超过会尝试增加到 2*当前使用内存。如果申请不到内存，则进行溢写。是 这个参数是 internal ，指定） 无效（见下方源码）。也就是说资源足够会自动扩容，所以不需要我们去设置。
2）溢写时使用输出流缓冲区默认 32k，这些缓冲区减少了磁盘搜索和系统调用次数，适当提高可以提升溢写效率。
3）shuffle 文件涉及到序列化，是采取批的方式读写，默认按照每批次 1 万条去读写。设置得太低会导致在序列化时过度复制，因为一些序列化器通过增长和复制的方式来翻倍内部数据结构。这个参数是 internal，指定无效。
#### 6.2 reduce端优化
##### 6.2.1 合理设置reduce数
过多的 cpu 资源出现空转浪费，过少影响任务性能。关于并行度、并发度的相关参数介绍，参照之前的介绍。
##### 6.2.2 输出产生小文件优化
1）join 结果插入新表
生成的文件数等于 shuffle 并行度，默认就是 200 份文件插入到hdfs 上(无分区)。
解决方式：

- 插入表数据之前进行缩小分区操作来解决小文件过多的问题，如coalesce，repartition算子。
- 调整shuffle并行度，根据之前的原则来设置。

2）有动态分区插入数据

1. 没有shuffle的情况下，最差的情况下，每个task中都有表各个分区的记录，那么最终文件数将达到task数*表分区数，这种情况下极容易产生小文件。
```
INSERT overwrite table A partition ( aa )
SELECT * FROM B;
```

2. 有 Shuffle 的情况下，上面的 Task 数量 就变成了 spark.sql.shuffle.partitions（默认值200）。那么最差情况就会有 spark.sql.shuffle.partitions * 表分区数。当 spark.sql.shuffle.partitions 设 置 过 大 时 ， 小 文 件 问 题 就 产 生 了 ； 当spark.sql.shuffle.partitions 设置过小时，任务的并行度就下降了，性能随之受到影响。
3. 最理想的情况是根据分区字段进行 shuffle，在上面的 sql 中加上 distribute by aa。把同一分区的记录都哈希到同一个分区中去，由一个 Spark 的 Task 进行写入，这样的话只会产生 N 个文件, 但是这种情况下也容易出现数据倾斜的问题。

解决思路：
结合解决倾斜的思路，在确定哪个分区键倾斜的情况下，将倾斜的分区键单独拎出来：将入库的 SQL 拆成（where 分区 != 倾斜分区键 ）和 （where 分区 = 倾斜分区键） 几个部分，非倾斜分区键的部分正常 distribute by 分区字段，倾斜分区键的部分 distribute by随机数，sql 如下：
```
//1.非倾斜键部分
INSERT overwrite table A partition ( aa )
SELECT *
FROM B where aa != 大 key
distribute by aa;
//2.倾斜键部分
INSERT overwrite table A partition ( aa )
SELECT *
FROM B where aa = 大 key
distribute by cast(rand() * 5 as int);
```
##### 6.2.3 增大reduce缓冲区
Spark Shuffle 过程中，shuffle reduce task 的 buffer 缓冲区大小决定了 reduce task 每次能够缓冲的数据量，也就是每次能够拉取的数据量，如果内存资源较为充足，适当增加拉取数据缓冲区的大小，可以减少拉取数据的次数，也就可以减少网络传输的次数，进而提升性能。reduce 端数据拉取缓冲区的大小可以通过spark.reducer.maxSizeInFlight 参数进行设置，默认为 48MB，一般是够用的，但是不能设置太大，因为。
我们可以通过shuffle read的读取时间来观测缓冲区大小对shuffle read的影响(橙色部分，上面为1m，下面为96m)，但是如果数据量不是很大，整体的收益是不大的。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173025092-b7ea74f5-9921-46ff-8cf4-6fba334bc87c.png#averageHue=%23e8f0d2&clientId=u8bea0fba-815c-4&from=paste&id=ucf4654f7&originHeight=488&originWidth=1788&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u10b23bfc-a2ad-41c8-a112-1933afe39f5&title=)
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173025215-d625aec6-1f98-487c-ad60-8a848b18aa88.png#averageHue=%23fbf9f7&clientId=u8bea0fba-815c-4&from=paste&id=u28a91bbb&originHeight=482&originWidth=762&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=ufc10bbe7-9a9c-4cfb-8a88-5046324b029&title=)
##### 6.2.4 调节reduce端拉取数据重试次数
Spark Shuffle 过程中，reduce task 拉取属于自己的数据时，如果因为网络异常等原因导致失败会自动进行重试。对于那些包含了特别耗时的 shuffle 操作的作业，建议增加重试最大次数（比如 6次），以避免由于 JVM 的 full gc 或者网络不稳定等因素导致的数据拉取失败。在实践中发现，对于针对超大数据量（数十亿~上百亿）的 shuffle 过程，调节该参数可以提高稳定性，但是我们也不能调整的太大，不能容忍一直失败下去。
参数：spark.shuffle.io.maxRetrie，默认为3。如果在指定次数之内拉取还是没有成功，那么就可能导致作业执行失败。建议使用6次。
##### 6.2.5 调节 reduce 端拉取数据等待间隔
Spark Shuffle 过程中，reduce task 拉取属于自己的数据时，如果因为网络异常等原因导致失败会自动进行重试，在一次失败后，会等待一定的时间间隔再进行重试，可以通过加大间隔时长（比如 60s），以增加 shuffle 操作的稳定性。
参数： spark.shuffle.io.retryWait 参数进行设置，默认值为 5s，建议60s。
##### 6.2.6 合理使用bypass
当ShuffleManager为SortShuffleManager的时候，如果满足以下的条件，可以使用bypass

- shuffle read task 的数量小于这个阈值（默认是 200）
- 不需要 map 端进行合并操作
- shuffle write 过程中不会进行排序操作

使用 BypassMergeSortShuffleWriter 去写数据，但是最后会将每个 task 产生的所有临时磁盘文件都合并成一个文件，并会创建单独的索引文件。
源码：SortShuffleManager.registerShuffle()
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173025264-96b9c57a-f7cd-48ff-96e5-a624b3f95c29.png#averageHue=%23fbf9f7&clientId=u8bea0fba-815c-4&from=paste&id=ue4c2aec0&originHeight=933&originWidth=729&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u307451f8-6494-4635-9b1b-89d284dad38&title=)
#### 6.3 整体优化
##### 6.3.1 数据本地化等待时长
在 Spark 项目开发阶段，可以使用 client 模式对程序进行测试，此时，可以在本地看到比较全的日志信息，日志信息中有明确的 Task 数据本地化的级别，如果大部分都是ROCESS_LOCAL(计算逻辑和数据在同一个jvm中)、NODE_LOCAL(数据和计算在同一个服务器上)，那么就无需进行调节，但是如果发现很多的级别都是RACK_LOCAL(数据和计算在相同的机架上)、ANY(数据和计算在不同的机架上)，那么需要对本地化的等待时长进行调节，应该是反复调节，每次调节完以后，再来运行观察日志，看看大部分的 task 的本地化级别有没有提升；看看，整个spark 作业的运行时间有没有缩短。
注意：**过犹不及，不要将本地化等待时长延长地过长，导致因为大量的等待时长，使得Spark 作业的运行时间反而增加了。**
下面几个参数，默认都是 3s，可以改成如下：
```
spark.locality.wait //是全局设置，下面的几个参考该值。建议 6s、10s
spark.locality.wait.process //建议 60s
spark.locality.wait.node //建议 30s
spark.locality.wait.rack //建议 20s
```
##### 6.3.2 使用堆外内存
1）堆外内存参数
讲到堆外内存，就必须去提一个东西，那就是去 yarn 申请资源的单位，容器。Spark on yarn 模式，一个容器到底申请多少内存资源。一个容器最多可以申请多大资源，是由 yarn 参数 yarn.scheduler.maximum-allocation-mb 决定， 需要满足：spark.executor.memoryOverhead + spark.executor.memory + spark.memory.offHeap.size≤ yarn.scheduler.maximum-allocation-mb
参数：

- spark.executor.memory：提交任务时指定的堆内内存。
- spark.executor.memoryOverhead：堆外内存参数，内存额外开销，默认开启，默认值是spark.executor.memory*0.1并且会和最小值384对比，取大值。所以如果spark on yarn任务堆内存申请1g，而实际取yarn申请的内存大于1g的原因
- spark.memory.offHeap.size：堆 外 内 存 参 数 ， spark 中 默 认 关 闭 ， 需 要 将spark.memory.enable.offheap.enable 参数设置为 true。

版本区别：YarnAllocator.scala

- 3.0以前： spark.executor.memoryOverhead 包含 spark.memory.offHeap.size。
- 3.0以后：去申请yarn的内存资源为3个参数相加。

2）使用堆外缓存
使用堆外内存可以减轻垃圾回收的工作，也加快了复制的速度。当需要缓存非常大的数据量时，虚拟机将承受非常大的 GC 压力，因为虚拟机必须检查每个对象是否可以收集并必须访问所有内存页。本地缓存是最快的，但会给虚拟机带来GC 压力，所以，当你需要处理非常多 GB 的数据量时可以考虑使用堆外内存来进行优化，因为这不会给 Java 垃圾收集器带来任何压力。让 JAVA GC 为应用程序完成工作，缓存操作交给堆外。
web ui上的Storage Memory指的是堆内的存储加执行+堆外的存储(堆外内存分为执行和存储，各占一半)。
##### 6.3.3 调节连接等待时长
在 Spark 作业运行过程中，Executor 优先从自己本地关联的 BlockManager(管理数据的) 中获取某份数据，如果本地 BlockManager 没有的话，会通过TransferService 远程连接其他节点上Executor 的 BlockManager 来获取数据。
如果 task 在运行过程中创建大量对象或者创建的对象较大，会占用大量的内存，这回导致频繁的垃圾回收，但是垃圾回收会导致工作现场全部停止，也就是说，垃圾回收一旦执行，Spark 的 Executor 进程就会停止工作，无法提供相应，此时，由于没有响应，无法建立网络连接，会导致网络连接超时。
在生产环境下，有时会遇到 file not found、file lost 这类错误，在这种情况下，很有可能是 Executor 的 BlockManager 在拉取数据的时候，无法建立连接，然后超过默认的连接等待时长 120s 后，宣告数据拉取失败，如果反复尝试都拉取不到数据，可能会导致 Spark 作业的崩溃。这种情况也可能会导致 DAGScheduler 反复提交几次 stage，TaskScheduler 反复提交几次 task，大大延长了我们的 Spark 作业的运行时间。
为了避免长时间暂停(如 GC)导致的超时，可以考虑调节连接的超时时长，连接等待时长需要在 spark-submit 脚本中进行设置，设置方式可以在提交时指定：--conf spark.core.connection.ack.wait.timeout=300s
### 7 spark3.0 AQE
Spark 在 3.0 版本推出了 AQE（Adaptive Query Execution），即自适应查询执行。AQE 是Spark SQL 的一种动态优化机制，在运行时，每当 Shuffle Map 阶段执行完毕，AQE 都会结合这个阶段的统计信息，基于既定的规则动态地调整、修正尚未执行的逻辑计划和物理计划，来完成对原始查询语句的运行时优化。
#### 7.1 动态合并分区
在 Spark 中运行查询处理非常大的数据时，shuffle 通常会对查询性能产生非常重要的影响。shuffle 是非常昂贵的操作，因为它需要进行网络传输移动数据，以便下游进行计算。
最好的分区取决于数据，但是每个查询的阶段之间的数据大小可能相差很大，这使得该数字难以调整：

- 如果分区太少，则每个分区的数据量可能会很大，处理这些数据量非常大的分区，可能需要将数据溢写到磁盘（例如，排序和聚合），降低了查询。
- 如果分区太多，则每个分区的数据量大小可能很小，读取大量小的网络数据块，这也会导致 I/O 效率低而降低了查询速度。拥有大量的 task（一个分区一个 task）也会给Spark 任务计划程序带来更多负担。

为了解决这个问题，我们可以在任务开始时先设置较多的 shuffle 分区个数，然后在运行时通过查看 shuffle 文件统计信息将相邻的小分区合并成更大的分区。
案例：
假设正在运行 select max(i) from tbl group by j。输入 tbl 很小，在分组前只有 2个分区。那么任务刚初始化时，我们将分区数设置为 5，如果没有 AQE，Spark 将启动五个任务来进行最终聚合，但是其中会有三个非常小的分区，为每个分区启动单独的任务这样就很浪费。取而代之的是，AQE 将这三个小分区合并为一个，因此最终聚只需三个 task 而不是五个。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173025522-48591782-9b6b-4d2e-93fa-51750081449d.png#averageHue=%23baad5f&clientId=u8bea0fba-815c-4&from=paste&id=uaa2e493d&originHeight=291&originWidth=882&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=udb5f09c7-d716-4b0e-b855-451e49d4663&title=)
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173025706-09ea503b-097a-4ca4-9b97-e86e0a8d04a1.png#averageHue=%23bbaf63&clientId=u8bea0fba-815c-4&from=paste&id=u1ca0dbb6&originHeight=201&originWidth=828&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u55e0b206-15ab-4cd8-b324-8456d49b505&title=)
#### 7.2 动态切换Join策略
Spark 支持多种 join 策略，其中如果 join 的一张表可以很好的插入内存，那么broadcast shah join 通常性能最高。因此，spark join 中，如果小表小于广播大小阀值（默认10mb），Spark 将计划进行 broadcast hash join。但是，很多事情都会使这种大小估计出错（例如，存在选择性很高的过滤器），或者 join 关系是一系列的运算符而不是简单的扫描表操作。
为了解决此问题，AQE 现在根据最准确的 join 大小运行时重新计划 join 策略。从下图实例中可以看出，发现连接的右侧表比左侧表小的多，并且足够小可以进行广播，那么AQE 会重新优化，将 sort merge join 转换成为 broadcast hash join。
对于运行是的 broadcast hash join，可以将 shuffle 优化成本地 shuffle，优化掉 stage 减少网络传输。Broadcast hash join 可以规避 shuffle 阶段，相当于本地 join。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173026140-98c69556-5c85-4b3f-b77d-ba73e4849383.png#averageHue=%23ebf2e9&clientId=u8bea0fba-815c-4&from=paste&id=u5c787268&originHeight=309&originWidth=884&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=ub4de3339-c684-48a4-8998-d4e8593325e&title=)
#### 7.3 动态优化Join倾斜
当数据在群集中的分区之间分布不均匀时，就会发生数据倾斜。严重的倾斜会大大降低查询性能，尤其对于 join。AQE skew join 优化会从随机 shuffle 文件统计信息自动检测到这种倾斜。然后它将倾斜分区拆分成较小的子分区。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173026349-601de37b-c9c4-46ef-82c1-5d69772e4eaa.png#averageHue=%23faf5ef&clientId=u8bea0fba-815c-4&from=paste&id=ue36ea8dc&originHeight=777&originWidth=1044&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u9ff4cf96-458a-4a60-bf23-dfcf68ede4a&title=)
没有这种优化，会导致其中一个分区特别耗时拖慢整个 stage,有了这个优化之后每个task 耗时都会大致相同，从而总体上获得更好的性能。
3.0有了AQE机制，就可以交给Spark自行解决，Spark3.0增加了以下参数：

- spark.sql.adaptive.skewJoin.enabled：是否开启倾斜 join 检测，如果开启了，那么会将倾斜的分区数据拆成多个分区,默认是开启的，但是得打开 aqe。
- spark.sql.adaptive.skewJoin.skewedPartitionFactor：默认值 5，此参数用来判断分区数据量是否数据倾斜，当任务中最大数据量分区对应的数据量大于的分区中位数乘以此参数，并且也大于 spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 参数，那么此任务是数据倾斜。
- spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes :默认值 256mb，用于判断是否数据倾斜。
- spark.sql.adaptive.advisoryPartitionSizeInBytes :此参数用来告诉 spark 进行拆分后推荐分区大小是多少。

如果同时开启了 spark.sql.adaptive.coalescePartitions.enabled 动态合并分区功能，那么会先合并分区，再去判断倾斜。
### 8 Spark3.0 DPP
Spark3.0 支持动态分区裁剪 Dynamic Partition Pruning，简称 DPP，核心思路就是先将join 一侧作为子查询计算出来，再将其所有分区用到 join 另一侧作为表过滤条件，从而实现对分区的动态修剪。如下图所示：
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173026435-f8cb5041-44eb-4b66-8df2-fa5448f80b0f.png#averageHue=%23fbfbfb&clientId=u8bea0fba-815c-4&from=paste&id=u63a53f36&originHeight=282&originWidth=1003&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u338d53ec-78b2-47b5-84d1-d750d739dde&title=)
```
将
select t1.id,t2.pkey from t1 join t2 on t1.pkey =t2.pkey and t2.id<2 
优化成了 
select t1.id,t2.pkey from t1 join t2 on t1.pkey=t2.pkey and t1.pkey in(select t2.pkey from t2 where t2.id<2)
```
触发条件：

1. 待裁剪的表 join 的时候，join 条件里必须有分区字段
2. 如果是需要修剪左表，那么 join 必须是 inner join ,left semi join 或 right join,反之亦然。但如果是 left out join,无论右边有没有这个分区，左边的值都存在，就不需要被裁剪。
3. 另一张表需要存在至少一个过滤条件，比如 a join b on a.key=b.key and a.id<2
### 9 Spark3.0 Hint增强
在 spark2.4 的时候就有了 hint 功能，不过只有 broadcasthash join 的 hint,这次 3.0 又增加了 sort merge join,shuffle_hash join,shuffle_replicate nested loop join。
Spark 的 5 种 Join 策略：[https://www.cnblogs.com/jmx-bigdata/p/14021183.html](https://www.cnblogs.com/jmx-bigdata/p/14021183.html)
1）broadcasthast join
```
sparkSession.sql("select /*+ BROADCAST(school) */ * from test_student
student left join test_school school on student.id=school.id").show()
sparkSession.sql("select /*+ BROADCASTJOIN(school) */ * from
test_student student left join test_school school on
student.id=school.id").show()
sparkSession.sql("select /*+ MAPJOIN(school) */ * from test_student
student left join test_school school on student.id=school.id").show()
```
2）sort merge join
```
sparkSession.sql("select /*+ SHUFFLE_MERGE(school) */ * from
test_student student left join test_school school on
student.id=school.id").show()
sparkSession.sql("select /*+ MERGEJOIN(school) */ * from test_student
student left join test_school school on student.id=school.id").show()
sparkSession.sql("select /*+ MERGE(school) */ * from test_student
student left join test_school school on student.id=school.id").show()
```
3）shuffle_hash join
```
sparkSession.sql("select /*+ SHUFFLE_HASH(school) */ * from test_student
student left join test_school school on student.id=school.id").show()
```
4）shuffle_replicate_nl join
使用条件非常苛刻，驱动表（school 表）必须小,且很容易被 spark 执行成 sort merge join。
```
sparkSession.sql("select /*+ SHUFFLE_REPLICATE_NL(school) */ * from
test_student student inner join test_school school on
student.id=school.id").show()
```
### 10 故障排除
#### 10.1 控制 reduce 端缓冲大小以避免 OOM
		在 Shuffle 过程，reduce 端 task 并不是等到 map 端 task 将其数据全部写入磁盘后再去拉取，而是 map 端写一点数据，reduce 端 task 就会拉取一小部分数据，然后立即进行后面的聚合、算子函数的使用等操作。		reduce 端 task 能够拉取多少数据，由 reduce 拉取数据的缓冲区 buffer 来决定，因为拉取过来的数据都是先放在 buffer 中，然后再进行后续的处理，buffer 的默认大小为 48MB。
		reduce 端 task 会一边拉取一边计算，不一定每次都会拉满 48MB 的数据，可能大多数时候拉取一部分数据就处理掉了。		虽然说增大 reduce 端缓冲区大小可以减少拉取次数，提升 Shuffle 性能，但是有时map 端的数据量非常大，写出的速度非常快，此时 reduce 端的所有 task 在拉取的时候，有可能全部达到自己缓冲的最大极限值，即 48MB，此时，再加上 reduce 端执行的聚合函数的代码，可能会创建大量的对象，这可难会导致内存溢出，即 OOM。		如果一旦出现 reduce 端内存溢出的问题，我们可以考虑减小 reduce 端拉取数据缓冲区的大小，例如减少为 12MB。		在实际生产环境中是出现过这种问题的，这是典型的以性能换执行的原理。reduce 端拉取数据的缓冲区减小，不容易导致 OOM，但是相应的，reudce 端的拉取次数增加，造成更多的网络传输开销，造成性能的下降。注意，要保证任务能够运行，再考虑性能的优化。
#### 10.2 JVM GC 导致的 shuffle 文件拉取失败
		在 Spark 作业中，有时会出现 shuffle file not found 的错误，这是非常常见的一个报错，有时出现这种错误以后，选择重新执行一遍，就不再报出这种错误。		出现上述问题可能的原因是 Shuffle 操作中，后面 stage 的 task 想要去上一个 stage 的task 所在的 Executor 拉取数据，结果对方正在执行 GC，执行 GC 会导致 Executor 内所有的工作现场全部停止，比如 BlockManager、基于 netty 的网络通信等，这就会导致后面的task 拉取数据拉取了半天都没有拉取到，就会报出 shuffle file not found 的错误，而第二次再次执行就不会再出现这种错误。
		可以通过调整 reduce 端拉取数据重试次数和 reduce 端拉取数据时间间隔这两个参数来对 Shuffle 性能进行调整，增大参数值，使得 reduce 端拉取数据的重试次数增加，并且每次失败后等待的时间间隔加长。
```
val conf = new SparkConf()
.set("spark.shuffle.io.maxRetries", "60")
.set("spark.shuffle.io.retryWait", "60s")
```
#### 10.3 解决各种序列化导致的报错
当 Spark 作业在运行过程中报错，而且报错信息中含有 Serializable 等类似词汇，那么可能是序列化问题导致的报错。
注意以下三点：

- 作为 RDD 的元素类型的自定义类，必须是可以序列化的
- 算子函数里可以使用的外部的自定义变量，必须是可以序列化的
- 不可以在 RDD 的元素类型、算子函数里使用第三方的不支持序列化的类型，例如Connection。
#### 10.4 解决算子函数返回 NULL 导致的问题
在一些算子函数里，需要我们有一个返回值，但是在一些情况下我们不希望有返回值，此时我们如果直接返回 NULL，会报错，例如 Scala.Math(NULL)异常
如果你遇到某些情况，不希望有返回值，那么可以通过下述方式解决：

- 返回特殊值，不返回 NULL，例如“-1”；
- 在通过算子获取到了一个 RDD 之后，可以对这个 RDD 执行 filter 操作，进行数据过滤，将数值为-1 的数据给过滤掉；
- 在使用完 filter 算子后，继续调用 coalesce 算子进行优化。
#### 10.5 解决 YARN-CLIENT 模式导致的网卡流量激增问题
YARN-client 模式的运行原理：
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173026460-fe721575-78e9-4dd8-8cbb-6ff835b0920a.png#averageHue=%23f0eeec&clientId=u8bea0fba-815c-4&from=paste&id=u368c16fd&originHeight=528&originWidth=901&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u5e282356-8806-4db9-ae3f-ff1422a5fb5&title=)
在 YARN-client 模式下，Driver 启动在本地机器上，而 Driver 负责所有的任务调度，需要与 YARN 集群上的多个 Executor 进行频繁的通信。
假设有 100 个 Executor， 1000 个 task，那么每个 Executor 分配到 10 个 task，之后，Driver 要频繁地跟 Executor 上运行的 1000 个 task 进行通信，通信数据非常多，并且通信品类特别高。这就导致有可能在 Spark 任务运行过程中，由于频繁大量的网络通讯，本地机器的网卡流量会激增。
YARN-client 模式只会在测试环境中使用，而之所以使用 YARN-client 模式，是由于可以看到详细全面的 log 信息，通过查看 log，可以锁定程序中存在的问题，避免在生产环境下发生故障。
生产环境下：
使用的一定是 YARN-cluster 模式。在 YARN-cluster 模式下，就不会造成本地机器网卡流量激增问题，如果 YARN-cluster 模式下存在网络通信的问题，需要运维团队进行解决。
#### 10.6 YARN-CLUSTER 模式的 JVM 永久代内存溢出无法执行问题？？？
YARN-cluster 模式的运行原理：
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709173026595-ab2adb19-0016-482e-8d23-3c97ce1b483e.png#averageHue=%23efeceb&clientId=u8bea0fba-815c-4&from=paste&id=ub36226de&originHeight=347&originWidth=953&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u883801b7-6b42-4d34-9207-7f2586fd3a3&title=)
当 Spark 作业中包含 SparkSQL 的内容时，可能会碰到 YARN-client 模式下可以运行，但是 YARN-cluster 模式下无法提交运行（报出 OOM 错误）的情况。
YARN-client 模式下，Driver 是运行在本地机器上的，Spark 使用的 JVM 的 PermGen 的配置，是本地机器上的 spark-class 文件，JVM 永久代的大小是 128MB，这个是没有问题的，但是在 YARN-cluster 模式下，Driver 运行在 YARN 集群的某个节点上，使用的是没有经过配置的默认设置，PermGen 永久代大小为 82MB。
SparkSQL 的内部要进行很复杂的 SQL 的语义解析、语法树转换等等，非常复杂，如果sql 语句本身就非常复杂，那么很有可能会导致性能的损耗和内存的占用，特别是对PermGen 的占用会比较大。
所以，此时如果 PermGen 的占用好过了 82MB，但是又小于 128MB，就会出现 YARN-client 模式下可以运行，YARN-cluster 模式下无法运行的情况。
解决上述问题的方法时增加 PermGen 的容量，需要在 spark-submit 脚本中对相关参数进行设置：--conf spark.driver.extraJavaOptions="-XX:PermSize=128M -XX:MaxPermSize=256M"
通过上述方法就设置了 Driver 永久代的大小，默认为 128MB，最大 256MB，这样就可以避免上面所说的问题。
#### 10.7 解决 SparkSQL 导致的 JVM 栈内存溢出
当 SparkSQL 的 sql 语句有成百上千的 or 关键字时，就可能会出现 Driver 端的 JVM 栈内存溢出。JVM 栈内存溢出基本上就是由于调用的方法层级过多，产生了大量的，非常深的，超出了 JVM 栈深度限制的递归。（我们猜测 SparkSQL 有大量 or 语句的时候，在解析 SQL 时，例如转换为语法树或者进行执行计划的生成的时候，对于 or 的处理是递归，or 非常多时，会发生大量的递归）
此时，建议将一条 sql 语句拆分为多条 sql 语句来执行，每条 sql 语句尽量保证 100 个以内的子句。根据实际的生产环境试验，一条 sql 语句的 or 关键字控制在 100 个以内，通常不会导致 JVM 栈内存溢出。
#### 10.8 持久化与checkpoint的使用
Spark 持久化在大部分情况下是没有问题的，但是有时数据可能会丢失，如果数据一旦丢失，就需要对丢失的数据重新进行计算，计算完后再缓存和使用，为了避免数据的丢失，可以选择对这个 RDD 进行 checkpoint，也就是将数据持久化一份到容错的文件系统上（比如 HDFS）。
一个 RDD 缓存并 checkpoint 后，如果一旦发现缓存丢失，就会优先查看 checkpoint 数据存不存在，如果有，就会使用 checkpoint 数据，而不用重新计算。也即是说，checkpoint可以视为 cache 的保障机制，如果 cache 失败，就使用 checkpoint 的数据。
使用 checkpoint 的优点在于提高了 Spark 作业的可靠性，一旦缓存出现问题，不需要重新计算数据，缺点在于，checkpoint 时需要将数据写入 HDFS 等文件系统，对性能的消耗较大。
#### 10.9 内存泄露排查
内存泄露是指程序中已动态分配的堆内存由于某种原因程序未释放或无法释放，造成系统内存的浪费，导致程序运行速度减慢,甚至系统崩溃等严重后果。
在 Spark Streaming 中往往会因为开发者代码未正确编写导致无法回收或释放对象，造成 Spark Streaming 内存泄露越跑越慢甚至崩溃的结果。那么排查内存泄露需要一些第三方的工具，例如：IBM HeapAnalyzer
#### 10.10 频繁GC问题
1）打印GC详情
统计一下 GC 启动的频率和 GC 使用的总时间，在 spark-submit 提交的时候设置参数
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
如果出现了多次 Full GC，首先考虑的是可能配置的 Executor 内存较低，这个时候需要增加 Executor Memory 来调节。
2）如果一个任务结束前，Full GC 执行多次，说明老年代空间被占满了，那么有可能是没有分配足够的内存
```
1.调整 executor 的内存，配置参数 executor-memory
2.调整老年代所占比例：配置-XX:NewRatio 的比例值
3.降低 spark.memory.storageFraction 减少用于缓存的空间
```
3）如果有太多 Minor GC，但是 Full GC 不多，可以给 Eden 分配更多的内存。
```
1.比如 Eden 代的内存需求量为 E，可以设置 Young 代的内存为-Xmn=4/3*E,设置该值也会导致Survivor 区域扩张
2.调整 Eden 在年轻代所占的比例，配置-XX:SurvivorRatio 的比例值
```
4）调整垃圾回收器，通常使用 G1GC，即配置-XX:+UseG1GC。当 Executor 的堆空间比较大时，可以提升 G1 region size(-XX:G1HeapRegionSize)，在提交参数指定：
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
 
