package com.common

import java.sql.Connection

import com.typesafe.config.ConfigFactory
import com.util.DBUtil
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable._

/**
  * Created by yxl on 17/4/14.
  */
class MySQLOffsetsStore extends OffsetsStore {

  override def readOffsets(topic: String,consumer:String): Option[Map[TopicPartition, Long]] = {
      var offsetSQLConnection : Connection = null
      try{
          val conf = ConfigFactory.load("config_dev.conf")
          offsetSQLConnection = DBUtil.createMySQLConnectionFactory(conf.getString("mysql_offset.url"),
              conf.getString("mysql_offset.userName"), conf.getString("mysql_offset.password"))
          val sql = s"select topic,`partition`,offset from kafka_consumer_offset where topic ='$topic'"
          val statement = offsetSQLConnection.createStatement()
          val resultSet = statement.executeQuery(sql)
          val rowsSeq = DBUtil.resultSet2Seq(resultSet)
          val offset = rowsSeq match {
              case None => None
              case Some(rows) => {
                  val offsetMap = rows.foldLeft(Map[TopicPartition, Long]())((map, row) => {
                      val topic = row.getOrElse("topic", "").toString
                      val partition = row.getOrElse("partition", -1)
                      val offset = row.getOrElse("offset", -1)
                      if (topic != null && partition != null && offset != null
                      && partition.toString.toInt != -1 && offset.toString.toInt != -1) {
                          val topicPartition = new TopicPartition(topic, partition.toString.toInt)
                          map.+=(topicPartition -> offset.toString.toInt)
                      }
                      map
                  })
                  Some(offsetMap)
              }
          }
          offset
      }catch {
          case e @ (_:RuntimeException) => throw new Exception(e)
      }finally{
          if(offsetSQLConnection != null){
              offsetSQLConnection.close()
          }
      }

  }

    override def saveOffsets(topic: String, consumer: String, offsetRanges: Array[OffsetRange]): Unit = {
    }
}
