package yarn

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/12/30.
 */
object TestSaveParquet {

   def main(args:Array[String]): Unit ={


     val sparkConf = new SparkConf().setAppName("TestMultipleOutput")
       .setMaster("local[4]")
       //.set("spark.cores.max", "1")
       //.set("spark.executor.memory", "128m")
       //.set("spark.driver.memory", "128m")
       .set("spark.local.dir", "/Users/yxl/data/spark.dir")

     val sc = new SparkContext(sparkConf)


     //val dataRDD = sc.textFile("/Users/yxl/data/data.txt");

     val rdd = sc.textFile("hdfs://linux1:8020/data/data.txt");

     val output = "hdfs://linux1:8020/output/ten"


     val job = new Job(sc.hadoopConfiguration)

    // ParquetOutputFormat.setWriteSupportClass(job, classOf[DeprecatedParquetOutputFormat])





   }

}
