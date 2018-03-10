package api

import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by yxl on 17/1/3.
  */
object TestPartitionCount {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestPartitionCount")
      .setMaster("local[*]")
     // .set("spark.cores.max", "1").set("spark.executor.memory", "128m")
     // .set("spark.driver.memory", "128m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(Seq("a","b","c","d","e","f","g"),3)

    def getIteratorSize[T](iterator: Iterator[T]): Long = {
      var count = 0L
      while (iterator.hasNext) {
        count += 1L
        iterator.next()
      }
      count
    }

    val array = rdd.context.runJob(
      rdd,
      getIteratorSize _ ,
      0 until rdd.partitions.length - 1 // do not need to count the last partition
    ).scanLeft(0L)(_ + _)

   array.foreach(println(_))


   rdd.mapPartitionsWithIndex((partition,iter) => {
        val result = ListBuffer[(Int,String)]()
        while(iter.hasNext){
            result.append((partition,iter.next()))
        }
        result.toIterator
   }).map(x => (x._1,x._2)).groupByKey().mapValues(x => x.size).foreach(println)

  }

}
