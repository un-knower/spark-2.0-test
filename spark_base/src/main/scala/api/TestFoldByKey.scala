package api

import org.apache.spark._

import scala.collection.mutable.ListBuffer

/**
 * Created by yxl on 15/9/14.
 */
object TestFoldByKey {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestFoldByKey")
      .setMaster("local[2]")
      .set("spark.cores.max", "1")
      .set("spark.driver.memory", "128m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")
    val sc = new SparkContext(sparkConf);

    val data=sc.parallelize(List((1, "www"),(1,"iteblog"),(2, "iteblog"),(2,"x"),(2,"test")))


    data.partitions

    val countPartition = (p:Int,iter:Iterator[(Int,String)]) =>{
         iter.map(t => (p,1))
    }

    println(data.mapPartitionsWithIndex(countPartition).countByKey())

    def fun(v1:String,v2:String) = v1 + "\t" + v2

    val listBuffer = new ListBuffer[String]()

    val seqOp = (listBuffer:ListBuffer[String],x:String) => {
        listBuffer.+=(x)
    }

    val Op = (listBuffer1:ListBuffer[String],listBuffer2:ListBuffer[String]) => {
        listBuffer1.++=(listBuffer2)
    }

    val foldRDD = data.aggregateByKey(listBuffer)(seqOp,Op)

    foldRDD.foreach(println(_))

  }

}
