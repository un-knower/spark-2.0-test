package example

import java.util.concurrent.TimeUnit

import base.StructuredBase
import org.apache.commons.lang3.StringUtils
import state.Event
import util.DateUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * Created by yxl on 2018/3/20.
  */
object OutputSinkMode extends StructuredBase {

    override protected val appName: String = "OutputSinkMode"

    def main(args: Array[String]): Unit = {
        import spark.implicits._

        super.addListener(format = true)

        val lines = super.socketData

        val eventDataFrame = lines.filter(line => {
            StringUtils.isNotEmpty(line)
        })
        .map(line => { // user, timestamp
            val array = line.split(",")
            val timestamp = DateUtil.dateStr2Timestamp(array(1).trim)
            Event(array(0), timestamp)
        })

        // 在 event-time watermark 字段上进行聚合
        //val eventAggregation = eventDataFrame
        //.withWatermark("timestamp", "1 minute")
        //.groupBy("timestamp")
        //.agg(count("sessionId").as("session_count"))

        // 在其他字段上聚合
        val eventAggregation = eventDataFrame
        .withWatermark("timestamp", "1 minute")
        .groupBy("sessionId")
        .agg(max("timestamp"))

        val writeStream = eventAggregation.writeStream
        .outputMode(OutputMode.Update())
        .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
        .format("console")
        .queryName(this.appName)

        val query = writeStream.start()

        query.awaitTermination()

    }

}
