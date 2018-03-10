package com.example

import com.common.Log
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yxl on 2017/8/9.
  */
object RunException extends Log {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("RunException")

        sparkConf.setMaster("local[3]")

        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        sparkConf.set("spark.sql.shuffle.partitions","5")

        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val lines = ssc.socketTextStream("localhost", 9999)

        val words = lines.flatMap(line =>{
            if(line.length > 1 ){
                throw new RuntimeException("line exception")
            }
            line.split(" ")
        })

        val pairs = words.map(word => (word, 1))
        val wordCounts = pairs.reduceByKey(_ + _)

       // wordCounts.print()

        wordCounts.foreachRDD(
            rdd => {
                val spark  = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
                // single sparkContext
                assert(spark.sparkContext == ssc.sparkContext)
            }
        )

        ssc.start()

        ssc.awaitTermination()


    }

}
