package client.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
  * Created by yxl on 16/12/7.
  */
object ClientBaseConsumer {

    def main(args: Array[String]): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("group.id", "aaaa")
        props.put("auto.offset.reset", "earliest")
        props.put("enable.auto.commit", "true")
        props.put("auto.commit.interval.ms", "1000")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        val consumer = new KafkaConsumer[String, String](props)
        consumer.subscribe(Seq("state-direct-kafka").asJavaCollection)
        while (true) {
            val records: ConsumerRecords[String, String] = consumer.poll(100)
            val it = records.iterator()
            while (it.hasNext()) {
                println(it.next().value())
            }
        }

    }

}
