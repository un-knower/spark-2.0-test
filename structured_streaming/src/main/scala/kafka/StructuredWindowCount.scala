package kafka

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryStartedEvent, QueryTerminatedEvent, QueryProgressEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, OutputMode, ProcessingTime}
import org.apache.spark.sql.{Row, ForeachWriter, SparkSession}

/**
  * Created by yxl on 17/1/3.
  */
object StructuredWindowCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("StructuredWindowCount")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.local.dir", "/Users/yxl/data/spark.dir")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "structured-streaming-kafka-test")
      .option("startingoffsets", "earliest")
      .option("includeTimestamp", true)
      .load()
      .selectExpr("timestamp", "cast(value as string)")
      .toDF("timestamp", "value")
      .withWatermark("timestamp","10 minutes")

    val writeStream = lines.writeStream
      .foreach(new ForeachWriter[Row] {
        override def process(value: Row): Unit = {
          println(s"process:$value")
        }

        override def close(errorOrNull: Throwable): Unit = {
          println("close ...")
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          println(s"open partitionId:$partitionId  version:$version")
          true
        }
      })

    writeStream.outputMode(OutputMode.Append())
    writeStream.option("checkpointLocation", "file:///Users/yxl/data/spark.dir/checkpoint/window-kafka")
    writeStream.trigger(ProcessingTime.create(1, TimeUnit.MINUTES))
    writeStream.queryName("StructuredWindowCount")

    val query = writeStream.start()

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: QueryProgressEvent ")
      }
    })

    query.awaitTermination()

  }

}
