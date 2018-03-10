package kafka

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import base.StructuredBase
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._


/**
  * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic structured-streaming-kafka-test
  * bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic structured-streaming-kafka-test
  * 2018-03-02 16:18:07,hello
  *
  */
object WindowWaterMark extends StructuredBase {

    override protected val appName: String = "StructuredWindowCount"

    def main(args: Array[String]): Unit = {

        spark.sparkContext.setLogLevel("INFO")

        import spark.implicits._

        val lines = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "structured-streaming-kafka-test")
        .option("startingOffsets", "latest")
         //  kafka timestamp
         // .option("includeTimestamp", true)
         // .selectExpr("timestamp", "cast(value as string)")
        .load()
        .select("value") // 只获取value 字段
        .as[String]



        val worldCounts = lines
        .filter(line => {
            val array = line.split(",")
            array.size == 2
        })
        .map(line => { // 解析出 timestamp
            val array = line.split(",")
            val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            val dateTime = fmt.parseDateTime(array(0).trim)
            val timestamp = new Timestamp(dateTime.getMillis())
            println(s"line :($timestamp, ${array.mkString(",")})")
            (timestamp, array(1))
        })
        .toDF("timestamp","word") // 新的DF
        .withWatermark("timestamp", "1 minute")
        .groupBy("timestamp")
        //.groupBy($"word" ,window($"timestamp","1 minutes"))
        .count()
        //.orderBy(asc("timestamp"))

        println(s"streaming:${worldCounts.isStreaming}")


        spark.streams.addListener(new StreamingQueryListener() {
            override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
                println("Query started: " + queryStarted.id)
            }

            override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
                println("Query terminated: " + queryTerminated.id)
            }

            override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
                println(s"Query made progress: ${queryProgress.progress} ")
            }
        })

        val writeStream = worldCounts.writeStream
        .foreach(new ForeachWriter[Row] {
            override def process(value: Row): Unit = {
                println(s"process:$value")
            }

            override def close(errorOrNull: Throwable): Unit = {
                //println("close ...")
            }

            override def open(partitionId: Long, version: Long): Boolean = {
                //println(s"open partitionId:$partitionId  version:$version")
                true
            }
        })

        writeStream.outputMode(OutputMode.Update())
        //writeStream.outputMode(OutputMode.Append())
        writeStream.option("checkpointLocation", "file:///Users/yxl/data/spark/checkpoint/window-kafka")
        writeStream.trigger(Trigger.ProcessingTime(10,TimeUnit.SECONDS))
        writeStream.queryName("StructuredWindowCount")

        val query = writeStream.start()

        query.awaitTermination()

    }

}
