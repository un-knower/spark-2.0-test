package yarn

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, _}


/**
 * Created by yxl on 15/7/9.
 */
object TestReduceRDD {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TestReduceRDD")
      .setMaster("local[4]")
      //.set("spark.cores.max", "1")
      //.set("spark.executor.memory", "128m")
      .set("spark.driver.memory", "128m")
      //.set("spark.local.dir", "/Users/yxl/data/spark.dir")
      .set("spark.default.parallelism","3")
    val sc = new SparkContext(sparkConf);

    val dataRDD = sc.textFile("/Users/yxl/data/data.txt");
    //val dataRDD = sc.textFile("hdfs://linux1:8020/data/data.txt"); //HadoopRDD

    val mapRDD = dataRDD.flatMap(x => x.split(",").map(y => (y, 1))) // MapPartitionsRDD


    println(mapRDD.partitions.size)

    mapRDD.foreachPartition( partition => {
              println(1)
    })

//    val partitionRDD = mapRDD.repartition(3) // MapPartitionsRDD
//
//    val persistRDD = partitionRDD.persist(StorageLevel.MEMORY_AND_DISK) // cache rdd
//
//    val reduceRDD = persistRDD.reduceByKey((x1, x2) => x1 + x2)
//
//    val sortRDD = reduceRDD.sortByKey(true)
//
//    println(sortRDD.toDebugString)
//
//    sortRDD.collect().foreach(println(_))

    //reduceRDD.collect().foreach(println(_))

//    val output = "hdfs://linux1:8020/output/spark/TestRDD"
//
//    val outputPath = new Path(output)
//
//    val fs = outputPath.getFileSystem(sc.hadoopConfiguration)
//
//    if (fs.exists(outputPath)) {
//      fs.delete(outputPath, true)
//    }
//
//    reduceRDD.saveAsTextFile(output)

  }
}
