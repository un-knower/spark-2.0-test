package yarn

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.storage.StorageLevel


/**
 * Created by yxl on 15/7/9.
 */
object TestSortRDD {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TestSortRDD")
      //.setMaster("local[4]")
      //.set("spark.cores.max", "1")
      //.set("spark.executor.memory", "128m")
      //.set("spark.driver.memory", "128m")
      //.set("spark.local.dir", "/Users/yxl/data/spark.dir")
    val sc = new SparkContext(sparkConf);

    sc.setLogLevel("DEBUG")

   //val dataRDD = sc.textFile("/Users/yxl/data/data.txt");
   val dataRDD = sc.textFile("hdfs://linux1:8020/data/data.txt");

    //dataRDD.cache()

    val mapRDD = dataRDD.flatMap(x => x.split(",").map(y => (y, 1)))

    //dataRDD.persist(StorageLevel.DISK_ONLY)

    //val partitionRDD = mapRDD.repartition(3)

    val reduceRDD = mapRDD.reduceByKey((x1, x2) => x1 + x2)

    reduceRDD.persist(StorageLevel.DISK_ONLY)

    val sortRDD = reduceRDD.sortByKey(true)


    println(sortRDD.toDebugString)

    sortRDD.take(20).foreach(println(_))

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
