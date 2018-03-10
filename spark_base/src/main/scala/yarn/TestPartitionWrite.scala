package yarn

import java.io.{OutputStreamWriter, BufferedWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.io.File

/**
 * Created by yxl on 15/12/29.
 */
object TestPartitionWrite {

  def main(args:Array[String]): Unit = {
    def writeLines(basePath:String,iterator: Iterator[(String, String)]) = {
      val writers = new mutable.HashMap[String, BufferedWriter] // (key, writer) map
      try {
        while (iterator.hasNext) {
          val item = iterator.next()
          val key = item._1
          val line = item._2
          val writer = writers.get(key) match {
            case Some(writer) => writer
            case None =>
              val path = basePath + File.separator + key
              val outputStream = FileSystem.get(new Configuration()).create(new Path(path))
              val keyWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
              writers.put(key,keyWriter)
              keyWriter
          }
          writer.write(line + System.getProperty("line.separator"))
        }
      } finally {
        writers.values.foreach(_.close())
      }
    }

    val sparkConf = new SparkConf().setAppName("TestPartitionWrite")
     // .setMaster("local[4]")
    //.set("spark.cores.max", "1")
    //.set("spark.executor.memory", "128m")
    //.set("spark.driver.memory", "128m")
    //.set("spark.local.dir", "/Users/yxl/data/spark.dir")
    val sc = new SparkContext(sparkConf)


   // val rdd = sc.textFile("/Users/yxl/data/data.txt");

    val rdd = sc.textFile("hdfs://linux1:8020/data/data.txt");

    val output = "hdfs://linux1:8020/data/output/ten"

    class MyPartitioner(partitions: Int) extends Partitioner {
      //用 HashPartitioner 不行？
      override def numPartitions: Int = partitions

      override def getPartition(key: Any): Int = {
        // make sure lines with the same key in the same partition
        (key.toString.hashCode & Integer.MAX_VALUE) % numPartitions
      }
    }

    val partitions = rdd.map(x => {
      val array = x.split(",")
      (array(0), array(0) + "," + array(1))
    }).partitionBy(new HashPartitioner(5))

    partitions.foreachPartition(writeLines(output,_))






  }



}
