package example

import java.util.concurrent.TimeUnit

import base.StructuredBase
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQueryListener, Trigger}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

/**
  * Created by yxl on 2018/3/10.
  */
object DelayTime extends StructuredBase {
    override protected val appName: String = "DelayTime"

    def main(args: Array[String]): Unit = {

        import spark.implicits._

        spark.streams.addListener(new StreamingQueryListener() {
            override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
                println("Query started: " + queryStarted.id)
            }
            override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
                println("Query terminated: " + queryTerminated.id)
            }
            override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
                 println("Query made progress: " + queryProgress.progress)
            }
        })

        val lines = spark.readStream
        .format("socket")
        .option("host","localhost")
        .option("port", 9999)
        .load()
        .as[String]

        val words = lines
        .map(x => {
            TimeUnit.MINUTES.sleep(1)
            x
        }).groupBy("value").count()


        val writeStream = words.writeStream
        .outputMode(OutputMode.Complete())
        .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
        .queryName("DelayTime")
        .format("console")

        val query = writeStream.start()

        query.awaitTermination()

        spark.stop()


    }
}
