package common

import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.kafka010.OffsetRange
import scala.collection.mutable._

/**
  * Created by yxl on 17/4/14.
  */
trait OffsetsStore {

  @transient val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def readOffsets(topic: String): Option[Map[TopicPartition, Long]]

  def saveOffsets(topic:String,offsetRanges:Array[OffsetRange]): Unit

}
