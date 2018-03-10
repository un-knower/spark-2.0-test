package yarn

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by yxl on 15/12/28.
 */
object TestMultipleOutput {


  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String]
  }



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

     val output = "hdfs://linux1:8020/data/output/ten"


     val mapRDD = rdd.map(x =>{
       val array = x.split(",")
       (array(0),array(0) + "," + array(1))
     })

     mapRDD.saveAsHadoopFile(output,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])

   //  mapRDD.saveAsHadoopFile(output,classOf[String],classOf[String],classOf[DeprecatedParquetOutputFormat])


//     mapRDD.saveAsNewAPIHadoopFile(output,classOf[String],
//       classOf[String],classOf[ParquetOutputFormat[String]],sc.hadoopConfiguration)



   }

}
