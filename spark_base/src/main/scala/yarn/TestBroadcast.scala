package yarn

import org.apache.spark.storage.{StorageLevel, BroadcastBlockId}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/7/20.
 */
object TestBroadcast {

     def main(args:Array[String]) {

       val sparkConf = new SparkConf().setAppName("TestBroadcast")
         .setMaster("spark://localhost:7077")
         .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
         .set("spark.driver.memory", "128m")
         .set("spark.local.dir", "/Users/yxl/data/spark.dir")

       val sc = new SparkContext(sparkConf);

       val map = Map("0" -> "key0","1" -> "key1","2" -> "key2","3" -> "key3" )

       val bc = sc.broadcast(map)

       println("创建广播变量：" + BroadcastBlockId(bc.id))

       val visits = sc.parallelize((1 to 100).map(i => ((i%3).toString,1)))

       visits.persist(StorageLevel.DISK_ONLY)

       val reduce = visits.reduceByKey((x1,x2) => x1 + x2 )

       val joined = reduce.map(v => (v._1, (bc.value(v._1), v._2)))

       println(joined.toDebugString)

       joined.collect().foreach(println(_))

     }
}
