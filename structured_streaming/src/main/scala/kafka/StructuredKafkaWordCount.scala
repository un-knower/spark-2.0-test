package kafka

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.{Row, ForeachWriter, SparkSession}

/**
  * Created by yxl on 16/12/5.
  */
object StructuredKafkaWordCount {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
        .builder
        .master("local[2]")
        .appName("StructuredKafkaWordCount")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()

        import spark.implicits._

        // Create DataSet representing the stream of input lines from kafka
        val lines = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        //.option("startingoffsets", "latest")
        .option("subscribe", "structured-streaming-kafka-test")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]

        // Generate running word count
        // val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
        val wordCounts = lines.flatMap(_.split(" "))

//		val query = wordCounts.writeStream
//			.outputMode(OutputMode.Complete())
//			.option("checkpointLocation", "file:///Users/yxl/data/spark.dir/checkpoint/kafka")
//		    .format("console")
//		    .trigger(ProcessingTime.create(10,TimeUnit.SECONDS))
//		    .start()

        // 使用append 模式的前提是不能使用聚合
        val query = wordCounts.writeStream
        //.outputMode("complete")
        .outputMode("append")
        .option("checkpointLocation", "file:///Users/yxl/data/spark.dir/checkpoint/kafka")
        .foreach(new ForeachWriter[String] {
            override def open(partitionId: Long, version: Long): Boolean = {
                println(s"open  partitionId:$partitionId version:$version")
                true
            }

            override def process(value: String): Unit = {
                println(s"process row:$value")
            }

            override def close(errorOrNull: Throwable): Unit = {
                println("close")
            }
        })
        .trigger((ProcessingTime.create(1, TimeUnit.MINUTES)))
        .start()

        query.awaitTermination()
    }

}
