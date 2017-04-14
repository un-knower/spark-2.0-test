package kafka


import java.io.File

import _root_.common.{DateUtil, ZooKeeperOffsetsStore}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.Map


/**
  * Created by yxl on 17/4/10.
  *
  * 停止：
  * ps -ef | grep spark |  grep StateDirectKafkaWordCount | awk '{print $2}'   | xargs kill  -SIGTERM
  *
  */

object BeepertfTransEvent {

  @transient val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

    val conf = ConfigFactory.load("config_dev.conf")

    val sparkConf = new SparkConf().setAppName("StateDirectKafkaWordCount")
    sparkConf.setMaster(conf.getString("spark_streaming.spark_master"))
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(conf.getInt("spark_streaming.batch_duration")))

    val topic = conf.getString("consumer.topic")

    ssc.checkpoint(conf.getString("spark_streaming.spark_checkpoint") + File.separator + topic)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("consumer.bootstrap_servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> conf.getString("consumer.group_id"),
      "auto.offset.reset" -> conf.getString("consumer.offset_reset"),
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "heartbeat.interval.ms" -> (5 * 60 * 1000).toString,
      "session.timeout.ms" -> (5 * 60 * 1000 * 4).toString,
      "request.timeout.ms" -> (5 * 60 * 1000 * 5).toString
    )

    val shouldOffsetStore = conf.getBoolean("consumer.offset_store")

    var offsetMap = Map[TopicPartition, Long]()
    if (shouldOffsetStore) {
      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("consumer.zookeeper"))
      offsetMap = offsetsStore.readOffsets(topic).getOrElse(Map[TopicPartition, Long]())
    }

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq(topic), kafkaParams, offsetMap)
    )

    def parseMessage(message: String): Option[TransEventMessage] = {
      try {
        BeepertfMessage.parseMessage(message)
      } catch {
        case ex: Exception => {
          log.error(ex.printStackTrace())
          log.error(s"wrong format message:$message")
          None
        }
      }
    }

    stream.foreachRDD(rdd => {
      val valueRDD = rdd.map(line => { line.value()})

      if(conf.getBoolean("spark_streaming.save_hdfs")){
        val hdfs = conf.getString("hadoop.hdfs")
        val topicPath = hdfs + File.separator + topic + File.separator + DateUtil.getCurrentMills
        valueRDD.saveAsTextFile(topicPath)
      }

      val line = valueRDD.filter(line => { StringUtils.isNoneEmpty(line) && StringUtils.isNoneBlank(line) })
        .map(line => { line.trim() })
        .map(line =>  parseMessage(line))
        .filter(line => ! line.isEmpty)
        .map(line => line.get)
        .filter(line => "UPDATE".equals(line.eventType))
        .map(line => {
            line.copy(timestamp = DateUtil.getNextTenMinute(DateUtil.str2mills(line.timestamp)))
        })

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val eventDataFrame = line.toDF()
      eventDataFrame.createOrReplaceTempView("view_event_data")

      val sql =
        """
          | select
          |   adcId,
          |   timestamp,
          |   count(distinct(if(status='400',driverId,null))) as sign_driver,
          |   count(distinct(if(status='800',driverId,null))) as run_driver,
          |   count(distinct(if(status='900',driverId,null))) as complete_driver,
          |   sum(if(status = '900',eventPrice,0)) as event_price
          |   from view_event_data where isDel = '0' group by adcId,timestamp
        """.stripMargin

      val countDataFram = spark.sql(sql)

      countDataFram.show()

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offsetRange => {
        log.info(s"zookeeper offset: topic:${offsetRange.topic} partition:${offsetRange.partition} " +
          s"fromOffset:${offsetRange.fromOffset} endOffset:${offsetRange.untilOffset}")
      })
      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("consumer.zookeeper"))
      offsetsStore.saveOffsets(topic,offsetRanges)

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
