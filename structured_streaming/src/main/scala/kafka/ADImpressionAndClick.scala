package kafka

import base.StructuredBase
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import util.DateUtil

/**
  * 广告查看，点击情况
  * impression_id,timestamp,impression
  * click_id,timestamp,click
  *
  * 1,2018-03-06 10:52:07,impression
  * 1,2018-03-06 10:53:07,click
  *
  */
object ADImpressionAndClick extends StructuredBase {

    override protected val appName: String = "ADImpressionAndClick"


    def main(args: Array[String]): Unit = {

        import spark.implicits._

        super.addListener()

        val impressions = super.kafkaData(topic="structured-streaming-kafka-impression")
        .filter(line => StringUtils.isNotEmpty(line))
        .map(line =>{
            val array = line.split(",")
            (array(0),DateUtil.dateStr2Timestamp(array(1)))
        })
        .toDF("impression_id","impression_timestamp")
        .withWatermark("impression_timestamp", "1 hour")

        val clicks = super.kafkaData(topic="structured-streaming-kafka-click")
        .filter(line => StringUtils.isNotEmpty(line))
        .map(line =>{
            val array = line.split(",")
            (array(0),DateUtil.dateStr2Timestamp(array(1)))
        })
        .toDF("click_id","click_timestamp")
        .withWatermark("click_timestamp", "2 hours")

        val join = impressions.join(clicks,expr(
            """
              |click_id = impression_id
              |and click_timestamp > impression_timestamp
              |and click_timestamp <  impression_timestamp + interval 1 hour
            """.stripMargin),
            joinType = "left_outer")

        val query = consoleShow(join,10,OutputMode.Append())

        query.awaitTermination()



    }

}
