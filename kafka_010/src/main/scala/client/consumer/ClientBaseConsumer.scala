package client.consumer

import java.util.{UUID, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConverters._

/**
  * Created by yxl on 16/12/7.
  */
object ClientBaseConsumer {

    def main(args: Array[String]): Unit = {

        val topic = "binlog_beeper_tf_trans_event"
        val brokers = "kafka125.yn.com:9092,kafka126.yn.com:9092,kafka127.yn.com:9092"
        // val brokers = "localhost:9092"
        val props = new Properties()
        val id = UUID.randomUUID()
        props.put("bootstrap.servers", brokers)
        props.put("group.id", s"ClientBaseConsumer-$topic-$id")
        props.put("auto.offset.reset", "latest")
        props.put("enable.auto.commit", "false")
        props.put("auto.commit.interval.ms", "5000")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        val consumer = new KafkaConsumer[String, String](props)
        consumer.subscribe(Seq(topic).asJavaCollection)
        while (true) {
            val records: ConsumerRecords[String, String] = consumer.poll(100)
            val it = records.iterator()
            while (it.hasNext()) {
                println(it.next().value())
            }
        }

    }

}
