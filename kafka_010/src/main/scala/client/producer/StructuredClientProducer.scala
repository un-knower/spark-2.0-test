package client.producer

import java.util.Date

import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord, KafkaSerializer}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.util.{Success, Try}

/**
  * Created by yxl on 16/12/7.
  */
object StructuredClientProducer {

    def main(args: Array[String]): Unit = {
        val serializer = (msg: String) => msg.getBytes
        val kafkaPort = 9092
        val producerConfig: KafkaProducer.Conf[String, String] = {
            KafkaProducer.Conf(KafkaSerializer(serializer),
                KafkaSerializer(serializer),
                bootstrapServers = s"localhost:$kafkaPort",
                acks = "all")
        }
        val topic = "structured-streaming-kafka-test"
        val producer = KafkaProducer(producerConfig)

        val timestamp = new Date().getTime  // - 15 * 60 * 1000

        val message = s"$timestamp,spark-$timestamp"

        val record = KafkaProducerRecord(topic, Some(""), message)
//        producer.send(record).onComplete({
//            case Success(metadata) => {
//                println("partition:" + metadata.partition() + " offset:" + metadata.offset())
//            }
//        })
        producer.sendWithCallback(record)((metadata: Try[RecordMetadata]) => {
            metadata match {
                case Success(meta) =>
                    println("partition:" + meta.partition() + " offset:" + meta.offset())
            }

        })
        producer.flush()
        producer.close()
    }

}
