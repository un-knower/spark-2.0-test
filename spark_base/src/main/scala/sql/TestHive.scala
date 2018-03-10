package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yxl on 15/7/16.
 */
object TestHive {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestHive")
      //.setMaster("local[1]")
      //.set("spark.cores.max","2").set("spark.executor.memory","512m")
      //.set("spark.driver.memory","1024m")
      //.set("spark.local.dir","/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val schema = sqlContext.sql("show databases") ;

    schema.foreach(println(_))

  }

}
