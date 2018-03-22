package com.example

import base.StructuredBase
import monitor.MonitorServer
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

/**
  * Created by yxl on 2018/1/16.
  */

object AutoStop extends StructuredBase {

    override protected val appName: String = "AutoStop"

    def main(args: Array[String]): Unit = {

        //val spark = super.submitSpark

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

        val query = consoleShow(wordsCounts,10,OutputMode.Complete())

        import monitor.MonitorImplicits._

        query.monitorAndAwaitTermination()

        spark.stop()
    }
}
