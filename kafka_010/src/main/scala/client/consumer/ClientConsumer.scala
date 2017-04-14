package client.consumer

import cakesolutions.kafka.{KafkaDeserializer, KafkaConsumer}
import org.apache.kafka.clients.consumer.{OffsetResetStrategy, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConversions._
import scala.util.Random

/**
  * Created by yxl on 16/12/7.
  */
object ClientConsumer {

    def main(args: Array[String]): Unit = {

        def createConsumer() = {
            val deserializer = (bytes: Array[Byte]) => new String(bytes)
            def randomString: String = Random.alphanumeric.take(5).mkString("")

            val consumerConfig: KafkaConsumer.Conf[String, String] = {
                KafkaConsumer.Conf(KafkaDeserializer(deserializer),
                    KafkaDeserializer(deserializer),
                    bootstrapServers = s"localhost:9092",
                    groupId = randomString,
                    enableAutoCommit = true,
                    autoOffsetReset = OffsetResetStrategy.EARLIEST)
            }
            consumerConfig.withProperty("session.timeout.ms", "10000")
            val consumer = KafkaConsumer(consumerConfig)
            consumer
        }

        def processMessage(records: ConsumerRecords[String, String]): Unit = {
            val it = records.iterator()
            while (it.hasNext()) {
                val item = it.next()
                val cr = item.asInstanceOf[ConsumerRecord[String, String]]
                val partition = cr.partition()
                val offset = cr.offset()
                val value = cr.value()
                println(s"partition:$partition, offset:$offset, value:$value")
            }
        }

        val topic = "structured-streaming-kafka-test"

        def subscribe = {

            val consumer = createConsumer()
            consumer.subscribe(List(topic))
            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(100)
                processMessage(records)
            }
        }

        subscribe

        def assign = {
            val consumer = createConsumer()
            val partition0 = new TopicPartition(topic, 0)
            val partition1 = new TopicPartition(topic, 1)
            consumer.assign(List(partition0, partition1))
            while (true) {
                val records: ConsumerRecords[String, String] = consumer.poll(100)
                processMessage(records)
            }
        }

        // assign

    }

}
