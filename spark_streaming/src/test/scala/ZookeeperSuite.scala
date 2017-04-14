import common.ZooKeeperOffsetsStore
import org.apache.spark.streaming.kafka010.{OffsetRange, KafkaUtils}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/4/13.
  */
class ZookeeperSuite extends FunSuite with Matchers with BeforeAndAfter {

  val topic = "test-write-read-offset"

  val  zookeeperStore = new ZooKeeperOffsetsStore("localhost:2181")

  test("save offset") {

    val offsetRange = OffsetRange(topic,0,1,10)

    zookeeperStore.saveOffsets(topic,Seq(offsetRange).toArray)

  }

  test("read offset"){

    println(zookeeperStore.readOffsets(topic))

  }

}
