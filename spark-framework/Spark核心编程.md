## Spark核心编程
Spark计算框架为了能够进行高并发和高吞吐的数据处理，封装了三大数据结构，用于处理不同的应用场景，三大数据结构分别为：

- RDD：弹性分布式数据集。
- 累加器：分布式共享只写变量。
- 广播变量：分布式共享只读变量。
### 1 RDD
#### 1.1 什么是RDD？
RDD(Resilient Distributed Dataset)是弹性分布式数据集，是spark最基本的数据处理模型，代表一个弹性的，不可变的，可分区的，元素可并行计算的集合，实际只是封装了计算逻辑，实现了数据以及计算逻辑的分离。代码中是一个抽象类，它代表一个弹性的，不可变，可分区，里面的元素可并行计算的集合。
RDD的特点：

- 弹性：
   - 存储的弹性：内存与磁盘(shuffle)的自动切换
   - 容错的弹性：数据丢失可以自动恢复
   - 计算的弹性：计算出错重试机制
   - 分片的弹性：可根据需要重新分片，实际上我们理解为分区，也是为了提高并行度，我们也可以调整分区数，来适应我们的executor资源。
- 分布式：数据存储在大数据集群不同节点上，数据来源应该是分布式的环境中，例如HDFS。
- 数据集：RDD 只是封装了计算逻辑，并不保存数据
- 数据抽象：RDD 是一个抽象类，需要子类具体实现，例如HadoopRDD。
- 不可变：RDD 封装了计算逻辑，是不可以改变的，想要改变，只能产生新的 RDD，在新的 RDD 里面封装计算逻辑。
- 可分区、并行计算

word_count+图解：RDD中包含分区的概念，目的是形成task进行并行计算。
```scala
//transform data
val source: RDD[String] = sparkContext.textFile("spark-framework/spark-core/src/main/resources/data/wordcount/data.txt")
//scala自减原则
val result: RDD[(String, Int)] = source.flatMap(line => line.split(" ")).map(word => Tuple2.apply(word, 1)).reduceByKey(_ + _)
println(result.collect().mkString(","))
```

![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709202523401-8f4f91f1-cc40-44d0-b0c6-786f2aeb0b52.png#averageHue=%23cde1d0&clientId=ua0f26a93-128a-4&from=paste&height=699&id=ud6e273b3&originHeight=1048&originWidth=2357&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=850987&status=done&style=none&taskId=uc1fed82a-0859-408a-a44a-59e99bd6e4d&title=&width=1571.3333333333333)
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172934803-b223db7a-2e62-4141-929d-882faa9a17f1.png#averageHue=%23c8c9b5&clientId=ue460025c-44ab-4&from=paste&id=ub1ad7502&originHeight=351&originWidth=790&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u61e9e6b9-2b15-4a9c-bc0d-9ce1cc78f1a&title=)

- RDD数据只有调用collect方法(行动算子)的时候，才会真正的执行操作。
- RDD是不存数据；对比IO，IO因为存在缓冲区，因此会保存一部分数据。
#### 1.2 核心属性
```
Internally, each RDD is characterized by five main properties:
- A list of partitions
- A function for computing each split
- A list of dependencies on other RDDs
- Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
- Optionally, a list of preferred locations to compute each split on (e.g. block locations for an HDFS file)
```
1）分区列表
RDD 数据结构中存在分区列表，用于执行任务时并行计算，是实现分布式计算的重要属性。
```scala
/**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
protected def getPartitions: Array[Partition]
```
2）分区计算函数
Spark 在计算时，是使用分区函数对每一个分区进行计算，实际上计算方式是完全相同的
```scala
// =======================================================================
// Methods that should be implemented by subclasses of RDD
// =======================================================================

/**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
@DeveloperApi
def compute(split: Partition, context: TaskContext): Iterator[T]
```
3）RDD的依赖关系
RDD 是计算模型的封装，当需求中需要将多个计算模型进行组合时，就需要将多个 RDD 建立依赖关系
```scala
/**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   * A list of dependencies on other RDDs(获取当前Rdd的依赖关系列表)
   */
protected def getDependencies: Seq[Dependency[_]] = deps
```
4）分区器（可选）
当数据为 KV 类型数据时，可以通过设定分区器自定义数据的分区规则
```scala
/** Optionally overridden by subclasses to specify how they are partitioned. */
@transient val partitioner: Option[Partitioner] = None
```
5）首选位置（可选）
根据分区数据位置，将task分发给合适的节点==>移动数据不如移动计算(如果数据和计算在同一个节点，没有网络和磁盘IO那么代价肯定是比较小的)。
```scala
/**
   * Optionally overridden by subclasses to specify placement preferences.
   */
protected def getPreferredLocations(split: Partition): Seq[String] = Nil
```
#### 1.3 执行原理
计算的角度来看，数据处理过程中需要计算资源，包含内存和cpu以及计算模型(逻辑)，真正执行的时候需要将计算资源和计算模型进行协调。Spark框架在执行的时候，先申请资源，然后将应用程序的数据处理逻辑分解成计算任务，然后发送给分配资源的计算节点上，然后按照计算模型进行数据计算，最终得到计算结果。
1）启动yarn集群环境
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172934771-a249b292-f21d-4685-a48c-0c0fe70ccd50.png#averageHue=%23f2dada&clientId=ue460025c-44ab-4&from=paste&id=u2ebadf12&originHeight=302&originWidth=638&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u583429b5-a036-416b-b9bd-36f65098a07&title=)
2）Spark申请资源创建调度节点以及计算节点
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172934985-40386118-ea15-44dd-ac0d-741f64f92c46.png#averageHue=%23d8c58f&clientId=ue460025c-44ab-4&from=paste&id=u9611d641&originHeight=310&originWidth=661&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=uc864afcc-e3cd-42ea-a70a-3d733584f19&title=)
3）Spark框架根据需求将计算逻辑根据分区划分为不同的任务
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172934929-d8950cc9-5714-4cf3-9710-78de5b4cc803.png#averageHue=%23218828&clientId=ue460025c-44ab-4&from=paste&id=u8e5c440b&originHeight=304&originWidth=637&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u05fd82f6-12e3-47d5-ad17-74f0fb818b6&title=)
4）调用节点根据节点状态(首选位置等)发送task到指定的节点进行计算
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172934989-b89b5394-b5d7-406b-b4f4-f550e765e422.png#averageHue=%23e2e300&clientId=ue460025c-44ab-4&from=paste&id=u4ce4c489&originHeight=290&originWidth=530&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u9967752e-e637-4f9a-b218-3236dc7e5c7&title=)
RDD 在整个流程中主要用于逻辑的封装，并生成 Task 发送给Executor 节点执行计算。
### 2 基础编程
#### 2.1 创建RDD
##### 2.1.1 基于集合
```scala
  /**
   * parallelize or makeRDD. 
   * makeRDD在底层实际上是直接调用了parallelize.
   */
  def createRDDByMemory(sc: SparkContext): Unit = {
    val dataSeq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    val sourceRdd1: RDD[Int] = sc.parallelize(dataSeq)
    val sourceRdd2: RDD[Int] = sc.makeRDD(dataSeq)
    println(sourceRdd1.collect().mkString(","))
    println(sourceRdd2.collect().mkString(","))
  }

```
##### 2.1.2 基于文件
```scala
  /**
   * textFile: 以行的方式读取文件，支持文件路径，文件夹，通配符*，分布式文件系统。
   * wholeTextFiles: 以文件的形式读取文件，返回tuple格式的数据。key是文件名，value是文本内容。
   */
  private def createRDDByFile(sc: SparkContext): Unit = {
    val sourceRdd: RDD[String] = sc.textFile("spark-framework/spark-core/src/main/resources/data/wordcount/data.txt")
    val sourceRdd2: RDD[(String, String)] = sc.wholeTextFiles("spark-framework/spark-core/src/main/resources/data/wordcount/data.txt")
    println(sourceRdd.collect().mkString(","))
    println(sourceRdd2.collect().mkString(","))
  }
```
![image.png](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709212415152-600ead6b-75f6-43c7-a653-aaf4bc45a586.png#averageHue=%23fcfbfa&clientId=uf0dbaeee-a259-4&from=paste&height=149&id=u1d67bebd&originHeight=224&originWidth=1814&originalType=binary&ratio=1.5&rotation=0&showTitle=false&size=30326&status=done&style=none&taskId=ub0a04b9e-07b0-40f4-80cf-fd78a8da8b0&title=&width=1209.3333333333333)
##### 2.1.3 RDD的分区和并行度
_分区和并行度并不一样_
1）并行度
默认情况下，Spark 可以将一个作业切分多个任务后，发送给 Executor 节点并行计算，而能够并行计算的任务数量我们称之为并行度。makeRdd的第二个参数是分区数，如果不传递，就会使用集群默认的并行度作为分区数。
```scala
  /**
   * parallelize or makeRDD. makeRDD在底层实际上是直接调用了parallelize
   */
  private def createRDDByMemory(sc: SparkContext): Unit = {
    val dataSeq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    val sourceRdd1: RDD[Int] = sc.parallelize(dataSeq)
    val sourceRdd2: RDD[Int] = sc.makeRDD(dataSeq)
    println(sourceRdd1.collect().mkString(","))
    sourceRdd2.saveAsTextFile("spark-framework/spark-core/src/main/resources/data/makerdd");
  }

```
setMaster("local[*]")：代表核心数不受限，可以取到电脑最大的逻辑核心，如果我们配置了local[2]，那意味着允许使用两个逻辑核心
```scala
  /**
   * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }
```
LocalSchedulerBackend：默认并行度：从sparkConfig中获取参数spark.default.parallelism，如果没有配置，那么就使用电脑的逻辑核心数：假设我们电脑是2核4线程，那么生成的文件数量应该是4，默认的切片分区数是4。
```scala
override def defaultParallelism(): Int =
scheduler.conf.getInt("spark.default.parallelism", totalCores)
```
Hadoop RDDs, if we don`t given parallelism, then default min Parallelism is 2
```scala
/**
   * Default min number of partitions for Hadoop RDDs when not given by user
   * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
   * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
   */
def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
```
2）分区
1、读取内存集合数据，数据分区的规则：如果分区很多，但是数据不多，如何处理？？
```scala
  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.
  // UPDATE: A parallel collection can be checkpointed to HDFS, which achieves this goal.

  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }
```
核心逻辑：positions方法，对每个分区求出[start,end)。我们可以用一个例子来方便我们理解。
```java
private object ParallelCollectionRDD {
  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
   * is an inclusive Range, we use inclusive range for the last slice.
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      case r: Range =>
        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          } else {
            new Range.Inclusive(r.start + start * r.step, r.start + (end - 1) * r.step, r.step)
          }
        }.toSeq.asInstanceOf[Seq[Seq[T]]]
      case nr: NumericRange[T] =>
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices.toSeq
      case _ =>
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toSeq
        }.toSeq
    }
  }
}
```
总结：基于内存的方式创建的RDD，positions用于确定每个分区数据的起始和结束索引位置[start,end)；array.slice来切分数据中确定。比如数据为{1,2,3,4,5}。分区为3，那么每个分区的索引为[0,1), [1,3),[3,5)；所以切分·的数据为{1}, {2,3}, {4,5}
2、读取文件分区的探索
SparkContext.textFile读取文件是使用hadoop的文件读取方式(TextInputFormat)，每次读取数据是以行为单位的，和字节数是没有关系的。
数据读取的时候是以偏移量为单位。数据分区的偏移量范围的计算，0号分区：0=>[0,3]，1=>[3,6]，2=>[6,7]。我们读取的时候也可以设置最小分区数，但是真实的分区数是和数据相关的。
```scala
public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        StopWatch sw = (new StopWatch()).start();
        FileStatus[] stats = this.listStatus(job);
        job.setLong("mapreduce.input.fileinputformat.numinputfiles", (long)stats.length);
        long totalSize = 0L;
        boolean ignoreDirs = !job.getBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false) && job.getBoolean("mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs", false);
        List<FileStatus> files = new ArrayList(stats.length);
        FileStatus[] var9 = stats;
        int var10 = stats.length;

        for(int var11 = 0; var11 < var10; ++var11) {
            FileStatus file = var9[var11];
            if (file.isDirectory()) {
                if (!ignoreDirs) {
                    throw new IOException("Not a file: " + file.getPath());
                }
            } else {
                files.add(file);
                totalSize += file.getLen();
            }
        }

        long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits);
        long minSize = Math.max(job.getLong("mapreduce.input.fileinputformat.split.minsize", 1L), this.minSplitSize);
        ArrayList<FileSplit> splits = new ArrayList(numSplits);
        NetworkTopology clusterMap = new NetworkTopology();
        Iterator var15 = files.iterator();

        while(true) {
            while(true) {
                while(var15.hasNext()) {
                    FileStatus file = (FileStatus)var15.next();
                    Path path = file.getPath();
                    long length = file.getLen();
                    if (length != 0L) {
                        FileSystem fs = path.getFileSystem(job);
                        BlockLocation[] blkLocations;
                        if (file instanceof LocatedFileStatus) {
                            blkLocations = ((LocatedFileStatus)file).getBlockLocations();
                        } else {
                            blkLocations = fs.getFileBlockLocations(file, 0L, length);
                        }

                        if (this.isSplitable(fs, path)) {
                            long blockSize = file.getBlockSize();
                            long splitSize = this.computeSplitSize(goalSize, minSize, blockSize);

                            long bytesRemaining;
                            String[][] splitHosts;
                            for(bytesRemaining = length; (double)bytesRemaining / (double)splitSize > 1.1; bytesRemaining -= splitSize) {
                                splitHosts = this.getSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining, splitSize, clusterMap);
                                splits.add(this.makeSplit(path, length - bytesRemaining, splitSize, splitHosts[0], splitHosts[1]));
                            }

                            if (bytesRemaining != 0L) {
                                splitHosts = this.getSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining, bytesRemaining, clusterMap);
                                splits.add(this.makeSplit(path, length - bytesRemaining, bytesRemaining, splitHosts[0], splitHosts[1]));
                            }
                        } else {
                            if (LOG.isDebugEnabled() && length > Math.min(file.getBlockSize(), minSize)) {
                                LOG.debug("File is not splittable so no parallelization is possible: " + file.getPath());
                            }

                            String[][] splitHosts = this.getSplitHostsAndCachedHosts(blkLocations, 0L, length, clusterMap);
                            splits.add(this.makeSplit(path, 0L, length, splitHosts[0], splitHosts[1]));
                        }
                    } else {
                        splits.add(this.makeSplit(path, 0L, length, new String[0]));
                    }
                }

                sw.stop();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Total # of splits generated by getSplits: " + splits.size() + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
                }

                return (InputSplit[])splits.toArray(new FileSplit[splits.size()]);
            }
        }
    }
```
假设：我们读取一个文件，文件内容如下，文件的大小为7个字节(每行结束结束存在回车换行算为两个字节)，我们设置最小分区数为2。long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits); 意味着每个分区应该存放3个字节的数据

- 每个分区的字节大小：7/2=3。
- 计算分区数量：7/3=2余1，1/3=0.33，根据hadoop 1.1倍的切分数据原则。则需要加一个分区，所以分区的数量为2+1=3。
- 文件中的内容：读取文件的时候是按照offset来进行文件位置定位，前后都是闭，如果已经读取过，后面的分区不会重复读取。
   - 文件1：[0,3]：文件内容为：1CRLF2CRLF
   - 文件2：[3,6]：文件内容为：3
   - 文件3：[6,7]：文件内容为：空
```
1CRLF   0,1,2
2CRLF   3,4,5
3       6
```
代码：
```scala
  def testParallelizeByFile(sc: SparkContext): Unit = {
    val sourceRdd: RDD[String] = sc.textFile("spark-framework/spark-core/src/main/resources/data/wordcount/testParallelize.txt")
    sourceRdd.saveAsTextFile("spark-framework/spark-core/src/main/resources/data/output");
  }
```
分区截图：
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172935419-65063c5b-bcf8-40d5-b8c4-2fb0c11b05e9.png#averageHue=%23383f44&clientId=ue460025c-44ab-4&from=paste&id=ua6e37555&originHeight=224&originWidth=321&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u6291fc45-f799-4e5a-bfe5-85837930e03&title=)
假设2：如果最小分区为2，文件14个字节，内容如下，会如何产生分区呢？
```
1234567
89
0
```

1. 最小分区数为2，14/2=7，可以整除，所以实际分区数为2，每个分区7个字节的数据。
2. 分区内的文件内容：记住是以行为单位读取，已经读过的文件不会重复读取
   1. 分区0：[0，7]=>1234567，已经被读的数据不会再被读
   2. 分区1：[7,14]=>89CRLF0

如果数据源为多个文件：那么计算分区的时候是以文件为单位进行的分区
#### 2.2 RDD算子
RDD算子在只有转换操作的情况下不会执行，转换算子只是对RDD的逐层封装，扩展RDD的功能，只有遇到行动算子的时候才会触发任务的调度和作业的执行。
从认知心理学上认为解决问题实际上就是问题的状态进行改变：问题开始->操作(算子)->问题(中间态)->操作(算子)->问题解决
#### 2.3 转换算子
转换算子只是对Rdd的封装，扩展RDD的功能。RDD根据数据处理方式的不同将算子整体上分为：value类型，双value类型和key-value类型
##### 2.3.1 value
1、map
_map存在转换映射的概念，将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值得转换。例如单词统计的时候，我们将一个个的单词转换为tuple类型(word，1)，然后进行ReduceByKey进行数据的统计。_
```scala
/**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
  val cleanF = sc.clean(f)
  new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
}
```
test：将每一个元素的值乘以2。
```scala
val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4))

def mapFunction(num: Int): Int = {
  num * 2
}

val mapRDD: RDD[Int] = rdd.map(mapFunction)
mapRDD.collect().foreach(println)
//匿名函数
val mapRDD2 = rdd.map((num: Int) => {
  num * 2
})
mapRDD2.collect().foreach(println)
//匿名函数+自简原则:方法体只有一行省去{}；参数类型可推断，去除类型；参数只有一个，使用_代替
val mapRDD3 = rdd.map(_ * 2)
mapRDD3.collect().foreach(println)
sc.stop()
```
为什么说是并行计算？

- 如果分区数为1：分区内的数据有序执行，只有每一个数据走完全部的转换操作，才会执行下一个数据，这种方式的效率是比较低的。
- 如果分区数为多个：多个分区会并行执行，分区内有序，但是多个分区之间是没有顺序的。

test：有序执行
```scala
val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),1)
val map1: RDD[Int] = rdd.map(num => {
  println(">>>>>"+num)
  num
})
val map2: RDD[Int] = map1.map(num => {
  println("<<<<<"+num)
  num
})
map2.collect()
```
test：多个分区，分区之间无序执行
```scala
val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),3)
val map1: RDD[Int] = rdd.map(num => {
  println(">>>>>"+num)
  num
})
val map2: RDD[Int] = map1.map(num => {
  println("<<<<<"+num)
  num
})
map2.collect()

console:
>>>>>2
<<<<<2
>>>>>3
<<<<<3
>>>>>4
<<<<<4
>>>>>1
<<<<<1
```
2、mapPartitions
		以分区为单位将待处理的数据发送到计算节点进行处理，可以进行任意处理，哪怕是过滤数据。但是会将整个分区的数据加载到内存中进行引用，处理完的数据不会被释放掉，所以在内存比较小，数据量比较大的场景之下，很容易出现内存溢出。
函数签名：
```scala
/**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
def mapPartitions[U: ClassTag](
  f: Iterator[T] => Iterator[U],
  preservesPartitioning: Boolean = false): RDD[U] = withScope {
  val cleanedF = sc.clean(f)
  new MapPartitionsRDD(
    this,
    (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter),
    preservesPartitioning)
}
```
test：>>>>>只会打印两次，和分区的数量保持一致，每次拿一个分区的数据到内存中进行引用，处理完的数据不会被释放掉(存在引用)。
```scala
val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),2)
val map1: RDD[Int] = rdd.mapPartitions(iter => {
  println(">>>>>")
  iter.map(_*2)
})
map1.collect()
```
test：求每个分区的最大值
```scala
//返回每个分区中的最大值:应该是2和4
val map2: RDD[Int] = rdd.mapPartitions(iter => {
  List(iter.max).iterator
})
map2.collect().foreach(println)
```
和map的区别：

- 数据处理角度：map是分区内一个数据一个数据执行，类似于串行；mapPartition一次处理一个分区，类似于批操作。
- 性能角度：map分区内是串行操作，性能比较低；mapPartition性能比较高，但是会长时间占用内存，所以内存有限的情况下，不推荐使用。
- 功能：map逐个对元素进行操作，返回数不多不少；mapPartition返回迭代器对象，没有要求数量。

3、mapPartitionsWithIndex
函数签名：会传入分区的index
```scala
/**
 * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
 * of the original partition.
 *
 * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
 * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
 */
def mapPartitionsWithIndex[U: ClassTag](
  f: (Int, Iterator[T]) => Iterator[U],
  preservesPartitioning: Boolean = false): RDD[U] = withScope {
  val cleanedF = sc.clean(f)
  new MapPartitionsRDD(
    this,
    (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
    preservesPartitioning)
}
```
test：只要1号分区的数据
```scala
val map3: RDD[Int] = rdd.mapPartitionsWithIndex((index,iter) => {
  if(index==1){
    iter
  }else{
    //空的迭代器对象
    Nil.iterator
  }
})
```
test：返回每个数据的分区索引
```scala
val map4: RDD[(Int,Int)] = rdd.mapPartitionsWithIndex((index,iter) => {
  iter.map((index,_))
})
map4.collect().foreach(println)
```
4、flatMap
主要用于扁平映射
函数签名：
```scala
/**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
  val cleanF = sc.clean(f)
  new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.flatMap(cleanF))
}
```
test：合并两个list
```scala
val rdd: RDD[List[Int]] = sc.makeRDD(List[List[Int]](List(1,2),List(3,4)))
val flatRDD:RDD[Int] = rdd.flatMap(list => list)
flatRDD.collect().foreach(println)
```
test：拆分字符串
```scala
val rdd2: RDD[String] = sc.makeRDD(List("hello world","hello scala"))
val flatRDD2:RDD[String] = rdd2.flatMap(str => str.split(" "))
flatRDD2.collect().foreach(println)
```
test：合并
```scala
val rdd3 = sc.makeRDD(List(List(1, 2), 3, List(3, 4)))
val flatRDD3 = rdd3.flatMap {
  case list: List[_] => list
  case dat => List(dat)
}
flatRDD3.collect().foreach(println)
```
5、glom
将同一个分区的数据之间转换为相同类型的内存数组进行处理，分区不变，分区确定，当数据经过转换之后，分区是不会发生变化的。
函数签名：
```scala
/**
 * Return an RDD created by coalescing all elements within each partition into an array.
 */
def glom(): RDD[Array[T]] = withScope {
  new MapPartitionsRDD[Array[T], T](this, (_, _, iter) => Iterator(iter.toArray))
}
```
test：int==>array[Int]
```scala
//int==>array
val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),2)
val glom:RDD[Array[Int]] = rdd.glom()
//List[Array[int]]
glom.collect().foreach(data=>println(data.mkString(",")))
```
test：分区内取最大值，然后求和
```scala
val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),2)
val glomRDD2:RDD[Array[Int]] = rdd.glom()
var maxValues: RDD[Int] = glomRDD2.map(arr => {
  arr.max
})
println(maxValues.collect().sum)
```
6、groupBy
函数签名：按照指定的规则对数据进行分组，分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为shuffle，极限情况下，数据可能会被分到同一个分区中。一个组的数据在一个分区中，并不意味着一个分区中只可以有一个组。
```scala
/**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   * or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
  groupBy[K](f, defaultPartitioner(this))
}
```
test：区分奇数，偶数
```scala
val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4))
//区分奇数和偶数
val groupBy: RDD[(Int, Iterable[Int])] = rdd.groupBy(num => num % 2)
groupBy.collect().foreach(println)
```
test：冲apache.log中获取每个时间段的访问量
```scala
val lines: RDD[String] = sc.textFile("data/spark-core/operator/apache.log")
val timeRDD: RDD[(String, Iterable[(String, Int)])] = lines.map(line => {
  import java.text.SimpleDateFormat
  import java.util.Date
  val str: Array[String] = line.split(" ")
  val time: Date = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(str(3))
  (time.getHours.toString, 1)
}).groupBy(_._1)
//模式匹配
timeRDD.map({
  case (hour, iter) =>
  (hour, iter.size)
}
               ).collect().foreach(println)
```
7、filter
函数签名：根据过滤逻辑，筛选掉我们不需要的数据。但数据进行筛选过滤之后，分区不会发生改变，但是分区中的数据可能会不均衡，生产环境下，可能会出现数据倾斜(有的分区数据多，有的分区数据少)。
```scala
/**
 * Return a new RDD containing only the elements that satisfy a predicate.
 */
def filter(f: T => Boolean): RDD[T] = withScope {
  val cleanF = sc.clean(f)
  new MapPartitionsRDD[T, T](
    this,
    (_, _, iter) => iter.filter(cleanF),
    preservesPartitioning = true)
}
```
test：只要奇数
```scala
val data: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
data.filter(data => data % 2 != 0).collect().foreach(println)
```
test：从apache.log文件中获取2015年5月17日的请求路径。
```scala
val lines: RDD[String] = sc.textFile("data/spark-core/operator/apache.log")
lines.filter(line => {
  val strs: Array[String] = line.split(" ")
  strs(3).startsWith("17/05/2015")
}).map(line => {
  val strs: Array[String] = line.split(" ")
  strs(6)
}).collect().foreach(println)
```
8、sample
函数签名：
```scala
/**
   * Return a sampled subset of this RDD.
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be greater
   *  than or equal to 0
   * @param seed seed for the random number generator
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   * of the given [[RDD]].
   */
def sample(
  withReplacement: Boolean,
  fraction: Double,
  seed: Long = Utils.random.nextLong): RDD[T] = {
  require(fraction >= 0,
            s"Fraction must be nonnegative, but got ${fraction}")

  withScope {
    require(fraction >= 0.0, "Negative fraction value: " + fraction)
    if (withReplacement) {
      new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
    } else {
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
    }
  }
}
```

- 第一个参数的不同会影响到会不会被放回和使用哪种抽样器：
   - 如果为true：抽取放回，泊松。
   - 如果为false：抽取不放回，伯努利。
- 如果我们seed那么起始已经被确定了，如果不传递那么就是随机使用一个long的值作为种子

作用：当数据倾斜的时候可以使用这个函数来均衡数据
8、distinct
函数签名：
```scala
/**
 * Return a new RDD containing the distinct elements in this RDD.
 */
def distinct(): RDD[T] = withScope {
  distinct(partitions.length)
}

//本质
case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
```
9、coalesce
函数签名：缩减分区，如果每个分区的数据量太小了，我们可以缩减分区，减少分区调度的成本，默认情况下不会打乱分区中的数据(不shuffle)，只是会合并分区，因此可能会导致数据不平衡，出现数据倾斜，如果想要数据均衡，我们可以进行shuffle处理。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172935714-e58fa2ec-1194-4de2-841e-38fb96cb5256.png#averageHue=%23dcd8c6&clientId=ue460025c-44ab-4&from=paste&id=uc848cbd2&originHeight=342&originWidth=699&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u7e810aca-f6a6-4947-94d0-901e30675ad&title=)
思考：如果我们想要扩大分区如何处理？
不shuffle的情况下，数据不会被打乱，导致分区是无法扩展，但是我们一定要使用shuffle，否则是没有意义的。
10、repartition
函数签名：重新分区。该操作内部其实执行的是 coalesce 操作，参数 shuffle 的默认值为 true。无论是将分区数多的RDD 转换为分区数少的 RDD，还是将分区数少的 RDD 转换为分区数多的 RDD，repartition操作都可以完成，因为无论如何都会经 shuffle 过程。
```scala
/**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
  coalesce(numPartitions, shuffle = true)
}
```
11、sortBy
函数签名：排序之前，可以将数据通过f函数进行处理，之后按照f排序处理的结果进行排序，默认情况下升序排序且分区的数量是不变的，但是因为存在shuffle的过程，数据会被打乱重新组合。
```scala
/**
 * Return this RDD sorted by the given key function.
 */
def sortBy[K](
  f: (T) => K,
  ascending: Boolean = true,
  numPartitions: Int = this.partitions.length)
(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
  this.keyBy[K](f)
  .sortByKey(ascending, numPartitions)
  .values
}
```
##### 2.3.2 双value类型
1、intersection
函数签名：返回两者的交集。要求两边的数据源的数据类型保持一致。
2、union
函数签名：返回两者的并集(不会去重)。要求两边的数据源的数据类型保持一致。
3、subtract
函数签名：rdd1的角度，求差集。要求两边的数据源的数据类型保持一致。
4、zip
函数签名：拉链，两个rdd组合起来。不要求数据源类型保持一致，但是要求数据的分区和数据的个数要保持一致。
test:
```scala
val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
//3,4
val intersection: RDD[Int] = rdd1.intersection(rdd2)
intersection.collect().foreach(println)
println("-------------------")
//1,2,3,4,3,4,5,6不会去除重复的数据
val union: RDD[Int] = rdd1.union(rdd2)
union.collect().foreach(println)
println("-------------------")
//rdd1中有，rdd2没有的
val subtract: RDD[Int] = rdd1.subtract(rdd2)
subtract.collect().foreach(println)
println("-------------------")
//返回touple:(1,3),(2,4),(3,5),(4,6)
val zip: RDD[(Int, Int)] = rdd1.zip(rdd2)
zip.collect().foreach(println)
```
##### 2.3.3 key-value类型
1、partitionby
函数签名：根据指定的分区器进行分区
思考：

1. 如果重分区的分区其和当前RDD的分区器一样的情况怎么办？
   1. 如果类型相等，分区数量相等就什么都不会做，直接返回原来的RDD。
   2. 如果有不等的，就会产生新的RDD。
2. spark常见的分区器：
   1. HashPartitioner
   2. PythonPartitioner：只可以在特定的包下才可以使用
   3. RangePartitioner
3. 如果想要按照自己的方法进行分区怎么样？
   1. 我们可以自己实现分区器。

2、reduceByKey
函数签名：对相同的key来做聚合
3、groupByKey
函数签名：将key相同的数据分到统一个组中，形成对偶元组(String, Iterable[Int])
注意：

1. reduceByKey和groupBykey有什么区别？
   1. 性能上讲：reduceByKey的性能更好，主要还是从shuffle的角度来看。reduceBykey存在分区内预聚合，会减少shuffle的数据量，因此可能会稍微好一些。
   2. 从功能来说：groupBykey仅是分组，reduceByKey是分组聚合。
   3. 会导致数据打乱重组，存在shuffle操作，shuffle需要和磁盘进行交互，因此性能是非常低的。
#### 2.4 行动算子
_可以触发job运行的算子被称为行动算子。_
1.collect
将不同分区的数据按照**分区的顺序**采集到dirver端内存中，形成数组。
source code：实际上是调用sc的runJob方法，创建了一个ActiveJob并提交执行。
```scala
def collect(): Array[T] = withScope {
  val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  Array.concat(results: _*)
}
```
2.reduce
对数据进行两两聚合
### 3 核心编程
#### 3.1 序列化问题
#### 3.2 血缘依赖
1.什么是依赖关系，什么是血缘关系？
由A转换为B再转换为C，即相邻的两个RDD的关系我们称之为依赖关系，多个连续的RDD之间的关系我们称之为血缘关系。
2.有什么用处？
RDD是不会保存数据的，RDD为了提供容错性，需要将RDD的血缘关系保存下来，一旦出现错误，可以根据血缘关系将数据重新读取计算
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172935830-4e359414-9e5e-4ad0-a0de-fa6dfc3fa4d0.png#averageHue=%23eae8bd&clientId=ue460025c-44ab-4&from=paste&id=ue8de6faa&originHeight=459&originWidth=1011&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=ud208def7-2228-4c06-b658-1393191ce3c&title=)
3.如何查看呢？
1）rdd.toDebugString可以打印出血缘关系。
source code:
```scala
def main(args: Array[String]): Unit = {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
  val sparkContext = new SparkContext(sparkConf)
  val lines: RDD[String] = sparkContext.textFile("data/spark-core/wordcount")
  println(lines.toDebugString)
  val words: RDD[String] = lines.flatMap(_.split(" "))
  println(words.toDebugString)
  val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
  println(wordGroup.toDebugString)
  val word2Count = wordGroup.map {
    case (word, list) => {
      (word, list.size)
    }
  }
  println(word2Count.toDebugString)
  val result: Array[(String, Int)] = word2Count.collect()
  result.foreach(println)
  sparkContext.stop()
}
```
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172935719-fbd0db2f-aba5-4280-bc47-c106d62c68a6.png#averageHue=%23323130&clientId=ue460025c-44ab-4&from=paste&id=u336e9629&originHeight=531&originWidth=1494&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u324a4819-0988-4bdf-ab3b-73456729d06&title=)
2）rdd.dependencies可以打印出依赖关系。
demo code:
```scala
def main(args: Array[String]): Unit = {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
  val sparkContext = new SparkContext(sparkConf)
  val lines: RDD[String] = sparkContext.textFile("data/spark-core/wordcount")
  println(lines.dependencies)
  val words: RDD[String] = lines.flatMap(_.split(" "))
  println(words.dependencies)
  val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
  println(wordGroup.dependencies)
  val word2Count = wordGroup.map {
    case (word, list) => {
      (word, list.size)
    }
  }
  println(word2Count.dependencies)
  val result: Array[(String, Int)] = word2Count.collect()
  result.foreach(println)
  sparkContext.stop()
}
```
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172935758-c25073f0-9111-428b-b6da-2b13eb2af128.png#averageHue=%23333130&clientId=ue460025c-44ab-4&from=paste&id=u3bbeff39&originHeight=153&originWidth=1128&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u0be9aa82-75c3-468f-a414-f9ba2ca2ba7&title=)
OneToOneDependency：新的rdd的一个分区的数据依赖旧的rdd的一个分区的数据，有点像是独生子女，东西直接留给下一代，也可以称之为**窄依赖**。
```scala
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}
```
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172936164-e834ae59-0ced-4a0f-b325-57a8ef540856.png#averageHue=%23af8f8b&clientId=ue460025c-44ab-4&from=paste&id=uab6ef7dd&originHeight=455&originWidth=831&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=ua585cdc4-56bd-408e-9ffc-0f06aa5b3d7&title=)
ShuffleDependency：新的rdd的一个分区的数据依赖旧的rdd的多个分区的数据，会引起shuffle，我们形象的称之为**宽依赖**。
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172936719-fae0063b-8de8-442b-89e9-c84a77d64183.png#averageHue=%23c6b390&clientId=ue460025c-44ab-4&from=paste&id=u84d2cbfa&originHeight=452&originWidth=1023&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=ue9b39a4c-7a3f-430c-9f8d-58b5fe559ef&title=)
分区和task的数量也存在关系，如果是OneToOneDependency，则产生的task数量和分区数是一致的；如果存在shuffle的操作，那么task的数量会发生变化。
#### 3.3 RDD的阶段划分
_RDD存在阶段的概念，如果是1对1，则不需要再划分阶段，但是如果存在shuffle，那么就需要分阶段，存在等待的概念，即shuffle和阶段存在必然的联系，也就是stage。_
源代码：DAGScheduler::handleJobSubmitted。
DAGScheduler::createResultStage
```scala
private def createResultStage(
  rdd: RDD[_],
  func: (TaskContext, Iterator[_]) => _,
  partitions: Array[Int],
  jobId: Int,
  callSite: CallSite): ResultStage = {
  checkBarrierStageWithDynamicAllocation(rdd)
  checkBarrierStageWithNumSlots(rdd)
  checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
  val parents = getOrCreateParentStages(rdd, jobId)
  val id = nextStageId.getAndIncrement()
  val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
  stageIdToStage(id) = stage
  updateJobIdStageIdMaps(jobId, stage)
  stage
}
```
DAGScheduler::getOrCreateParentStages
```scala
/**
 * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
 * the provided firstJobId.
 */
private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
  getShuffleDependencies(rdd).map { shuffleDep =>
    getOrCreateShuffleMapStage(shuffleDep, firstJobId)
  }.toList
}
```
DAGScheduler::getOrCreateShuffleMapStage
```scala
/**
 * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
 * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
 * addition to any missing ancestor shuffle map stages.
 */
private def getOrCreateShuffleMapStage(
  shuffleDep: ShuffleDependency[_, _, _],
  firstJobId: Int): ShuffleMapStage = {
  shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
    case Some(stage) =>
    stage

    case None =>
    // Create stages for all missing ancestor shuffle dependencies.
    getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
      // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
      // that were not already in shuffleIdToMapStage, it's possible that by the time we
      // get to a particular dependency in the foreach loop, it's been added to
      // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
      // SPARK-13902 for more information.
      if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
        createShuffleMapStage(dep, firstJobId)
      }
    }
    // Finally, create a stage for the given shuffle dependency.
    createShuffleMapStage(shuffleDep, firstJobId)
  }
}
```
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172936600-b5c14d37-d25e-45af-a57a-2fdf3066a311.png#averageHue=%23c9ccc8&clientId=ue460025c-44ab-4&from=paste&id=u73f07899&originHeight=488&originWidth=862&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u7116aa54-8a3b-40e3-a873-fe26c5e5205&title=)
#### 3.4 任务划分
Rdd的任务划分中间分为：Application，Job，Stage，Task。Application->Job->Stage->Task每一层都是1对n的关系。

- Application：初始化一个SparkContext即生成一个Application。
- Job：一个Action算子就会生成一个job。
- Stage：Stage等于宽依赖(ShuffleDependency)的个数加1。
- Task：一个Stage阶段中，最后一个RDD的分区个数就是Task的个数。source code is DAGScheduler::submitMissingTasks
```scala
case stage: ResultStage =>
partitionsToCompute.map { id =>
  val p: Int = stage.partitions(id)
  val part = partitions(p)
  val locs = taskIdToLocations(id)
  new ResultTask(stage.id, stage.latestInfo.attemptNumber,
                       taskBinary, part, locs, id, properties, serializedTaskMetrics,
                       Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
                       stage.rdd.isBarrier())
}


/**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   *
   * This can only be called when there is an active job.
   */
override def findMissingPartitions(): Seq[Int] = {
  val job = activeJob.get
  (0 until job.numPartitions).filter(id => !job.finished(id))
}
```
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172936611-3f51dd71-4ffc-44b0-8f80-bb444da30e79.png#averageHue=%23f5f1eb&clientId=ue460025c-44ab-4&from=paste&id=ub9d50d05&originHeight=596&originWidth=1256&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u87192c67-3dd4-4287-80a4-f1aa18e2a40&title=)
#### 3.5 RDD的持久化
1.cache：
将数据临时的存储在内存中进行数据复用。
会添加一个CachedPartitions的一个依赖进去，可以通过查看血缘关系来获取
2.persist：
将数据临时存储在磁盘文件中进行数据复用，涉及到磁盘IO，性能比较低，但是数据安全。一旦作业执行完毕，临时保存的数据文件就会丢失。
3.checkpoint：
将数据长久的保存在磁盘文件中进行数据复用，涉及到磁盘IO，性能较低，但是数据安全，为了保证安全。一般情况下会独立执行作业。相当于又走了一次作业，一般情况是和cache联合使用的，先cache，再checkpoint。
```scala
val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd_Persist")
val sc = new SparkContext(sparkConfig)
sc.setCheckpointDir("cp")



//因为mapRdd需要被重用，而RDD中是不存储数据，为了避免重复执行，可以先执行持久化操作。
//mapRdd.cache()：存放到呃逆村中，实际上cache也是调用的persist,持久化操作必须再行动算子执行时完成的，也就是说只有需要行动算子来触发执行。
//检查点路径保存的文件，当文件执行完之后，不会被删除.一般的保存路径都是分布式存储路径.例如HDFS.
mapRdd.cache()
mapRdd.checkpoint()
```
check point 在执行过程会切断血缘关系，重新建立新的血缘关系。
#### 3.6 自定义分区器
Spark目前支持Hash分区和Range分区，和用户自定义分区，Hash分区为当前的默认分区，分区器直接决定了RDD中分区的个数，RDD中每条数据经过Shuffle后进入哪个分区，进而决定了Reduce的个数。

- 只有Keyvalue类型的RDD才有分区器，非Key-value类型的RDD的分区值是None。
- 每个RDD的分区ID范围：0~(numPartitions-1)，据欸的那个这个值是属于哪个分区的。

1）Hash分区
对于给定的key，计算hashCode值，并除以分区个数取余。
```scala
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
```
#### 3.7 累加器和广播变量
1.累加器：分布式共享只写变量
1）需求：如果我们想要实现一个简单的累加功能那我们可以如何操作呢？
```scala
//方式1：使用reduce
println(sourceData.reduce(_+_))
//方式2：遍历相加即可
//错误的方式
var sum = 0
sourceData.foreach(num => {
  sum += num
})
//sum = 0
println("sum = " + sum)

//正确的方式：使用累加器，可以在executor计算完成之后，再将结果返回给driver
val sum1: LongAccumulator = sc.longAccumulator("sum")
sourceData.foreach(num => {
  sum1.add(num)
})
//sum = 0
println("result = " + sum1.value)
```
累加器：用来将Executor端的变量信息聚合到driver端，在Driver程序中定义的变量，在Executor端的每个task都会得到这个变量的一份新的副本，每个task更新这些副本的值后，传回Driver端进行merge。
注：

- 如果在转换算子中调用累加器，如果没有行动算子，那么就不会执行。
```scala
val sum: LongAccumulator = sc.longAccumulator("sum")
sourceData.map(num=>{
  sum.add(num)
  num
})
//0
println(sum)
```

- 累加器是全局共享的，如果我们调用多次行动操作，会出现重复计算的问题。
```scala
val sum: LongAccumulator = sc.longAccumulator("sum")
val mapRdd: RDD[Int] = sourceData.map(num => {
  sum.add(num)
  num
})
mapRdd.collect()
mapRdd.collect()
//20
println(sum)
```

- 所以：一般情况下，我们会将累加器放到行动算子中操作。

2）自定义累加器实现wordcount(避免了reduceByKey的shuffle操作)
```scala
/**
 * 自定义累加器实现word count
 *
 * @author : code1997
 * @date : 2022/2/24 23:06
 */
object Rdd_Accumulator_WC {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seria_search")
    val sc = new SparkContext(sparkConfig)
    val data: RDD[String] = sc.makeRDD(List("hello", "java", "hello", "scala"))
    val accumulator = new MyAccumulator
    sc.register(accumulator, "wordCountAcc")
    data.foreach(word => {
      accumulator.add(word)
    })
    println(accumulator.value)
    sc.stop()
  }

  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    /**
     * 用于存储累加器中的数据
     */
    private val map: mutable.Map[String, Long] = mutable.Map[String, Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()

    override def reset(): Unit = map.clear()

    override def add(v: String): Unit = {
      val count: Long = map.getOrElse(v, 0L) + 1
      map.put(v, count)
    }

    //合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = map
      val map2 = other.value
      map2.foreach {
        case (word, count) => {
          val newCount: Long = map1.getOrElse(word, 0L) + count
          map1.put(word, newCount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = map
  }

}
```
2.广播变量
需求：实现两个数据集的join
1）使用join来实现：存在shuffle
```scala
val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
//join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
joinRdd.collect().foreach(println)
```
2）使用map实现：如果分区数很多，会产生比较多的task，以分区为单位的map数据的份数比较多，存在数据冗余
```scala
//使用map的方式就不会进行shuffle操作，但是假设我们有10个分区，但是只有1个executor,map又需要在每个分区上存在，那么就相当于冗余了10份，占用大量的内存
val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
rdd1.map {
  case (w, c) => {
    val l: Int = map.getOrElse(w, 0)
    (w, (c, l))
  }
}.collect().foreach(println)
```
3）使用广播变量：以executor为单位存储map数据
```scala
//改进：使用广播只读变量,executor实际上是一个JVM,所以在启动的时候，会自动分配内存，所以可以以executor为单位，将数据放到内存中，保存一份只读变量，降低冗余.
val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(mutable.Map(("a", 4), ("b", 5), ("c", 6)))
rdd1.map {
  case (w, c) =>
  (w, (c, bc.value.getOrElse(w, 0)))
}.collect().foreach(println)
```
### 4 案例
#### 4.1 分析
![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172936824-7f29116d-2668-4d7e-87b5-42af47f6b8bd.png#averageHue=%23272a32&clientId=ue460025c-44ab-4&from=paste&id=u381befa4&originHeight=386&originWidth=1336&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=ucb0ad899-4c48-4107-9839-4e71e8125bc&title=)

- 用户包含四种行为：搜索，点击，下单，支付。
- 数据文件中每行数据采用下划线分隔数据。
- 每一行数据表示用户的一次行为，这个行为只能是4种行为中的一种。
- 如果搜索关键字为null，表示数据不是搜索数据。
- 如果点击的品类ID和产品ID为-1，表示数据不是点击数据。
- 针对于下单行为，一次可以下单多个商品，所以品类ID和产品ID可以是多个，id之间使用逗号分隔，如果本次不是下单行为，则数据采用null表示。
- 支付行为和下单行为类似。
- 最后一个数表示城市id。

样例类：
```scala
/**
 * @author : code1997
 * @date : 2022/2/25 23:32
 */
case class UserVisitAction(
  date: String,
  user_id: Long,
  session_id: String,
  page_id: Long,
  action_time: String,
  search_keyword: String,
  click_category_id: Long,
  click_product_od: Long,
  order_category_ids: String,
  order_product_ids: String,
  pay_category_ids: String,
  pay_product_ids: String,
  city_id: String
)
```
#### 4.2 Top10热门品类
##### 4.2.1 实现
需求：读取数据文件，然后根据(点击，下单，支付)获取热度top10。
```scala
/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10Category {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc = new SparkContext(sparkConf)
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    //2.统计品类的点击数量：(品类ID，点击数量)
    val clickActionRdd: RDD[(String, Int)] = sourceData.filter(
      (action: String) => {
        action.split("_")(6) != "-1"
      }
    ).map((action: String) => {
      (action.split("_")(6), 1)
    }).reduceByKey((_: Int) + (_: Int))
    //3.统计品类的下单数量：(品类ID，下单数量)
    val orderActionRdd: RDD[(String, Int)] = sourceData.filter(
      (action: String) => {
        action.split("_")(8) != "null"
      }
    ).flatMap((action: String) => {
      action.split("_")(8).split(",")
    }).map((_: String, 1)).reduceByKey((_: Int) + (_: Int))
    //4.统计品类的支付数量：(品类ID，支付数量)
    val payActionRdd: RDD[(String, Int)] = sourceData.filter(
      (action: String) => {
        action.split("_")(10) != "null"
      }
    ).flatMap((action: String) => {
      action.split("_")(10).split(",")
    }).map((_: String, 1)).reduceByKey((_: Int) + (_: Int))
    //5.将品类进行排序，并且，取前10名(点击数量，下单数量，支付数量)，联想到元组的排序
    //将key相同的marge到一块
    //join，leftJoin?不行，我们需要(点击数量，下单数量，支付数量)三个都有值，有些品类可能是没有这些活动中的一项，会导致缺项的事情发生
    //cogroup：connect+group
    //todo:需要理解一下
    val categoryVisitRdd: RDD[(String, (Int, Int, Int))] = clickActionRdd
    .cogroup(orderActionRdd, payActionRdd)
    .mapValues {
      case (clickIter, orderIter, payIter) =>
      var clickCount = 0
      val clickIterator: Iterator[Int] = clickIter.iterator
      if (clickIterator.hasNext) {
        clickCount = clickIterator.next()
      }
      var orderCount = 0
      val orderIterator: Iterator[Int] = orderIter.iterator
      if (orderIterator.hasNext) {
        orderCount = orderIterator.next()
      }
      var payCount = 0
      val payIterator: Iterator[Int] = payIter.iterator
      if (payIterator.hasNext) {
        payCount = payIterator.next()
      }
      (clickCount, orderCount, payCount)
    }
    //按照(点击数量，下单数量，支付数量)的值降序排列
    val top10Rdd: Array[(String, (Int, Int, Int))] = categoryVisitRdd.sortBy(_._2, ascending = false).take(10)
    //6.采集结果到控制台打印出来.
    top10Rdd.foreach(println)
    sc.stop()
  }
}
```
##### ![](https://cdn.nlark.com/yuque/0/2024/png/29530415/1709172937118-44d8353d-30c7-444d-b975-dca41054be54.png#averageHue=%232e2d2d&clientId=ue460025c-44ab-4&from=paste&id=u2a4f9f08&originHeight=346&originWidth=1109&originalType=url&ratio=1.5&rotation=0&showTitle=false&status=done&style=none&taskId=u9d029464-2ad2-453a-8bc9-f32bcb82378&title=)4.2.2 优化
1）sourceRdd多次使用到
解决：使用cache，如果想要数据安全，可以使用checkpoint。
2）cogroup性能比较低
cogroup中可能存在shuffle操作，数据量比较大的情况下可能会影响性能。因此我们可以通过改变文件的数据结构来实现性能的提升。
```scala
/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10Category2 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    //Q1：sourceData读取三次，可以使用cache
    sourceData.cache()
    //2.统计品类的点击数量：(品类ID，点击数量)
    val clickActionRdd: RDD[(String, (Int, Int, Int))] = sourceData.filter(
      (action: String) => {
        action.split("_")(6) != "-1"
      }
    ).map((action: String) => {
      (action.split("_")(6), 1)
    }).reduceByKey((_: Int) + (_: Int)).map {
      case (id, cnt) => (id, (cnt, 0, 0))
    }

    //3.统计品类的下单数量：(品类ID，下单数量)
    val orderActionRdd: RDD[(String, (Int, Int, Int))] = sourceData.filter(
      (action: String) => {
        action.split("_")(8) != "null"
      }
    ).flatMap((action: String) => {
      action.split("_")(8).split(",")
    }).map((_: String, 1)).reduceByKey((_: Int) + (_: Int)).map {
      case (id, cnt) => (id, (0, cnt, 0))
    }

    //4.统计品类的下单数量：(品类ID，支付数量)
    val payActionRdd: RDD[(String, (Int, Int, Int))] = sourceData.filter(
      (action: String) => {
        action.split("_")(10) != "null"
      }
    ).flatMap((action: String) => {
      action.split("_")(10).split(",")
    }).map((_: String, 1)).reduceByKey((_: Int) + (_: Int)).map {
      case (id, cnt) => (id, (0, 0, cnt))
    }
    //5.将品类进行排序，并且，取前10名(点击数量，下单数量，支付数量)，联想到元组的排序
    //将key相同的marge到一块
    val unionRdd: RDD[(String, (Int, Int, Int))] = clickActionRdd.union(orderActionRdd).union(payActionRdd)
    unionRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    //按照(点击数量，下单数量，支付数量)的值降序排列
    val top10Rdd: Array[(String, (Int, Int, Int))] = unionRdd.sortBy(_._2, ascending = false).take(10)
    //6.采集结果到控制台打印出来.
    top10Rdd.foreach(println)
    val end = System.currentTimeMillis()
    printf("花费的时间为:%d", end - start)
    sc.stop()
  }
}
```
3）reduceByKey存在shuffle操作
通过减少reduceBykey操作的次数来提高性能。
```scala
def main(args: Array[String]): Unit = {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
  val sc = new SparkContext(sparkConf)
  val start = System.currentTimeMillis()
  //1.读取原始日志文件
  val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
  val flatRdd: RDD[(String, (Int, Int, Int))] = sourceData.flatMap(
    action => {
      val datas: Array[String] = action.split("_")
      if (datas(6) != "-1") {
        List((datas(6), (1, 0, 0)))
      } else if (datas(8) != "null") {
        val ids: Array[String] = datas(8).split(",")
        ids.map(id => (id, (0, 1, 0)))
      } else if (datas(10) != "null") {
        val ids: Array[String] = datas(10).split(",")
        ids.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    }
  )
  val resultRdd: RDD[(String, (Int, Int, Int))] = flatRdd.reduceByKey(
    (t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    }
  )
  //按照(点击数量，下单数量，支付数量)的值降序排列
  val top10Rdd: Array[(String, (Int, Int, Int))] = resultRdd.sortBy(_._2, ascending = false).take(10)
  //6.采集结果到控制台打印出来.
  top10Rdd.foreach(println)
  val end = System.currentTimeMillis()
  printf("花费的时间为:%d", end - start)
  sc.stop()
}
```
4）自定义累加器
使用累加器+foreach来代替reduceBykey
```scala
/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10Category4 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    //注册累加器
    val top10Accumulator = new Top10CategoryAccumulator
    sc.register(top10Accumulator)
    sourceData.foreach(
      (action: String) => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          top10Accumulator.add(datas(6), "click")
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            (id: String) => {
              top10Accumulator.add(id, "order")
            }
          )
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(
            (id: String) => {
              top10Accumulator.add(id, "pay")
            }
          )
        } else {
          Nil
        }
      }
    )
    val categories: mutable.Iterable[HotCategory] = top10Accumulator.value.map((_: (String, HotCategory))._2)
    categories.toList.sortWith(
      (l: HotCategory, r: HotCategory) => {
        if (l.clickCnt > r.clickCnt) {
          true
        } else if (l.orderCnt == r.orderCnt) {
          if (l.orderCnt > r.orderCnt) {
            true
          } else if (l.orderCnt == r.orderCnt) {
            l.payCnt >= r.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10).foreach(println)
    val end = System.currentTimeMillis()
    printf("花费的时间为:%d", end - start)
    sc.stop()
  }

  case class HotCategory(id: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int) {

  }

  /**
   * 自定义累加器
   */
  class Top10CategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val top10Map: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = top10Map.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new Top10CategoryAccumulator()

    override def reset(): Unit = top10Map.clear()

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2
      val category: HotCategory = top10Map.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      top10Map.update(cid, category)
    }


    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1: mutable.Map[String, HotCategory] = this.top10Map
      val map2: mutable.Map[String, HotCategory] = other.value
      map2.foreach {
        case (cid, hc) =>
        val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
        category.clickCnt += hc.clickCnt
        category.orderCnt += hc.orderCnt
        category.payCnt += hc.payCnt
        top10Map.put(cid, category)
      }
    }

    override def value: mutable.Map[String, HotCategory] = top10Map
  }

}
```
#### 4.3 sesson统计
_需求：Top10热门品类中每个品类的Top10活跃session统计。在需求一的基础之上，增加每个品类用户session的点击统计。_
```scala
/**
 * @author : code1997
 * @date : 2022/2/25 23:40
 */
object Top10CategorySession1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top10Category")
    val sc: SparkContext = new SparkContext(sparkConf)
    val start = System.currentTimeMillis()
    //1.读取原始日志文件
    val sourceData: RDD[String] = sc.textFile("data/spark-core/user_visit")
    sourceData.cache()
    val top10Ids: Array[String] = top10Category(sourceData)
    //过滤原始数据，保留点击和前10品类的id
    val filterdClickActionRdd: RDD[String] = sourceData.filter(
      action => {
        val datas: Array[String] = action.split("_")
        //只要top10的数据
        datas(6) != "-1" && top10Ids.contains(datas(6))
      }
    )
    //根据(品类id,session id)进行点击量的统计
    val result: RDD[(String, List[(String, Int)])] = filterdClickActionRdd.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _).map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }.groupByKey() //相同的品类分到一组
    .mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    result.foreach(println)
    val end = System.currentTimeMillis()
    printf("花费的时间为:%d", end - start)
    sc.stop()
  }

  /**
   * 获取热度top10的ids
   */
  def top10Category(sourceData: RDD[String]): Array[String] = {
    val flatRdd: RDD[(String, (Int, Int, Int))] = sourceData.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val resultRdd: RDD[(String, (Int, Int, Int))] = flatRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    //按照(点击数量，下单数量，支付数量)的值降序排列
    resultRdd.sortBy(_._2, ascending = false).take(10).map(_._1)
  }

}
```
#### 4.4 页面单跳转换率
_页面单跳转换率：比如一个用户在一次Session过程中访问的页面路径为3，5，7，9，21，那么页面之间的每一次跳转都叫单跳，比如计算3-5的单跳转换率，先获取符合条件的Session对于页面3的访问次数为A，然后获取符合条件的Session中访问页面3又紧接访问页面5的次数为B，那么B/A就是3-5的页面单跳转换率。_
### 5 三层架构
_良好的架构模式有助于项目的开发和维护。对于java中的存在MVC架构；大数据领域没有视图，则使用java的三层架构模式：controller，service，dao。_

 
