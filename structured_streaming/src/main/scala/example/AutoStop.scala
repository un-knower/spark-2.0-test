package com.example

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

/**
  * Created by yxl on 2018/1/16.
  */

class AutoStop extends  Closed {
    private var query : StreamingQuery = _

    def run(): Unit = {

        super.init()

        val spark = SparkSession.builder
        .master("local[2]")
        .appName("AutoStop")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.local.dir", "/Users/yxl/data/spark.dir")
        .getOrCreate()

        import spark.implicits._

        spark.streams.addListener(new StreamingQueryListener() {
            override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
                println("Query started: " + queryStarted.id)
            }
            override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
                println("Query terminated: " + queryTerminated.id)
            }
            override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
                // println("Query made progress: " + queryProgress.progress)
            }
        })

        val lines = spark.readStream
        .format("socket")
        .option("host","localhost")
        .option("port", 9999)
        .load()

        val words = lines.as[String].flatMap(_.split(" "))

        val wordsCounts = words.groupBy("value").count()

        val writeStream = wordsCounts.writeStream
        .outputMode(OutputMode.Complete())
        .trigger(ProcessingTime.create(2, TimeUnit.SECONDS))
        .queryName("AutoStop")
        .format("console")

        query = writeStream.start()

        query.awaitTermination()

        spark.stop()

    }

    override def stop(): Unit = {
        query.stop()

    }
}

object AutoStop {

    def main(args: Array[String]): Unit = {
         val autoStop = new AutoStop()
         autoStop.run()
    }

}
