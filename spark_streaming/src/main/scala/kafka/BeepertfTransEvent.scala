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
  * ps -ef | grep spark |  grep BeepertfTransEvent | awk '{print $2}'   | xargs kill  -SIGTERM
  *
  */

object BeepertfTransEvent {

  @transient val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

    val conf = ConfigFactory.load("config_pro.conf")

    val sparkConf = new SparkConf().setAppName("BeepertfTransEvent")
    sparkConf.setMaster(conf.getString("spark_streaming.spark_master"))
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.shuffle.partitions","5")

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
      offsetMap = offsetsStore.readOffsets(topic,conf.getString("consumer.group_id")).getOrElse(Map[TopicPartition, Long]())
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
        val topicPath = hdfs + File.separator + topic + File.separator + DateUtil.getDay + File.separator + DateUtil.getCurrentMills
        valueRDD.saveAsTextFile(topicPath)
      }

      val lines = valueRDD.filter(line => { StringUtils.isNoneEmpty(line) && StringUtils.isNoneBlank(line) })
        .map(line => { line.trim() })
        .map(line =>  parseMessage(line))
        .filter(line => line.nonEmpty)
        .map(line => line.get)
        .filter(line => "UPDATE".equals(line.eventType))
        .map(line => {
            line.copy(timestamp = DateUtil.getNextTenMinute(DateUtil.str2mills(line.timestamp)))
        })
        .filter(line => "0".equals(line.isDel))
        .coalesce(5)

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val eventDataFrame = lines.toDF()
      eventDataFrame.createOrReplaceTempView("view_event_data")

      // 计算 10min 签到司机, 在跑司机, 配送完成司机
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

      val countDriverDataFrame = spark.sql(sql)

      countDriverDataFrame.show() // todo 写入 hbase

      // 计算一个小时去重司机
      lines.foreachPartition(partition => {
         // redis connection

        // val redisConnection = ???
         partition.foreach(eventMessage => {
              val dateTime = DateUtil.dateStr2DateTime(eventMessage.timestamp)
              val dateTimeHour = dateTime.getHour
              val status = eventMessage.status
              val driverId = eventMessage.driverId
              // 2017-04-14_10@sign_driver ,  2017-04-14_10@run_driver , 2017-04-14_10@complete_driver  expire 1day
              val key = status match {
                case "400" => dateTimeHour + "@sign_driver"
                case "800" => dateTimeHour + "@run_driver"
                case "900" => dateTimeHour + "@complete_driver"
              }
         })
      })

      // 读取数据
      // redis connection
      lines.map(eventMessage => {
        val dateTime = DateUtil.dateStr2DateTime(eventMessage.timestamp)
        val dateTimeHour = dateTime.getHour
        dateTimeHour
      }).distinct()
        .collect()
        .foreach(timeHour => {
          val sign_driver_key = timeHour + "@sign_driver"
          val run_driver_key = timeHour + "@run_driver"
          val complete_driver_key = timeHour + "@complete_driver"
          // count
          // save hbase  60
      })


      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offsetRange => {
        log.info(s"zookeeper offset: topic:${offsetRange.topic} partition:${offsetRange.partition} " +
          s"fromOffset:${offsetRange.fromOffset} endOffset:${offsetRange.untilOffset}")
      })

      val offsetsStore = new ZooKeeperOffsetsStore(conf.getString("consumer.zookeeper"))
      offsetsStore.saveOffsets(topic,conf.getString("consumer.group_id"),offsetRanges)

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
