package common

/**
  * Created by yxl on 17/4/13.
  */
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._
import scala.collection.mutable._

object MyZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

class ZooKeeperOffsetsStore(zkHosts: String) extends OffsetsStore {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000,MyZKStringSerializer)

  override def readOffsets(topic: String): Option[Map[TopicPartition, Long]] = {

    val zkUtils = ZkUtils(zkClient,false)
    val topicPath = "/" + topic
    val (offsetsRangesStrOpt, _) = zkUtils.readDataMaybeNull(topicPath)

    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => ( new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap
        Some(Map() ++ offsets)
      case None =>
        None
    }

  }

  override def saveOffsets(topic:String,offsetRanges:Array[OffsetRange]): Unit = {

    val zkUtils = ZkUtils(zkClient,false)
    val topicPath = "/" + topic
    val offsetsRangesStr = offsetRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    log.info(s"Writing offsets to ZooKeeper: ${offsetsRangesStr}")
    zkUtils.updatePersistentPath(topicPath,offsetsRangesStr)
    zkUtils.deletePathRecursive(topicPath)
  }

}