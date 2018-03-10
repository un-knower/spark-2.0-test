package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import help.RDDExtend._

/**
  * Created by yxl on 15/7/16.
  */


object TestSQLStatic {


    case class Person(name: String, age: Int)

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("TestSQLStatic")
        //.setMaster("local[4]")
        //.set("spark.cores.max","3").set("spark.executor.memory","128m")
        //.set("spark.driver.memory","128m")
        //.set("spark.local.dir","/Users/yxl/data/spark.dir")

        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)


        import sqlContext.implicits._


        val recordRDD = sc.textFile("hdfs://myhost:8020/resources/people.txt")
        .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))


        val df = recordRDD.toDF()

        df.registerTempTable("records")

        df.printSchema()

        val result = sqlContext.sql("select * from records")

        val finalRDD = result.map(r => r(0) + "," + r(1)).rdd


        val output = "hdfs://myhost:8020/resources/out/merge"


        finalRDD.saveAsMergedTextFile(output, true)


    }


}

