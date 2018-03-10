package kafka

import java.util.concurrent.TimeUnit

import base.StructuredBase
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * Created by yxl on 2018/3/6.
  */
object StaticJoin extends StructuredBase {
    override protected val appName: String = "StaticJoin"

    def main(args: Array[String]): Unit = {

        import spark.implicits._

        val staticDF = spark.sparkContext.parallelize(Seq(
            ("a","1"),
            ("b","2"),
            ("c","3"),
            ("d","4")
        )).toDF("key","value")

        val lines = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "structured-streaming-kafka-test")
        .option("startingOffsets", "latest")
        .load()
        .select("value") // 只获取value 字段
        .as[String]

        val words = lines
        .filter(line => StringUtils.isNotEmpty(line))
        .map(line => line.trim)
        .flatMap(line => line.split(" "))
        .toDF("word")
        .dropDuplicates("word")

        val join = words.join(staticDF, $"word" === $"key", "leftouter").select("word","value")

        val writeStream = join.writeStream
        .foreach(new ForeachWriter[Row] {
            override def process(value: Row): Unit = {
                println(s"process:$value")
                throw new Exception("user exception")
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
        writeStream.option("checkpointLocation", "file:///Users/yxl/data/spark/checkpoint/static-join")
        writeStream.trigger(Trigger.ProcessingTime(10,TimeUnit.SECONDS))
        writeStream.queryName("StaticJoin")

        val query = writeStream.start()

        query.awaitTermination()


    }
}
