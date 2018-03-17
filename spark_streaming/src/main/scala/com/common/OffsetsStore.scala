package com.common

import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.kafka010.OffsetRange
import scala.collection.mutable._

/**
  * Created by yxl on 17/4/14.
  */
trait OffsetsStore extends Log with Serializable  {

  def readOffsets(topic: String,consumer:String): Option[Map[TopicPartition, Long]]

  def saveOffsets(topic:String,consumer:String,offsetRanges:Array[OffsetRange]): Unit

}
