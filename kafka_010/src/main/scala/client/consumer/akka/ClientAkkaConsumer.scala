package client.consumer.akka

import akka.actor.Actor.Receive
import akka.actor.{Props, ActorSystem, Actor, ActorLogging}
import akka.event.LoggingReceive
import cakesolutions.kafka.akka.KafkaConsumerActor.Subscribe.AutoPartition
import cakesolutions.kafka.{KafkaDeserializer, KafkaConsumer}
import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe}
import cakesolutions.kafka.akka.{ConsumerRecords, KafkaConsumerActor, Offsets}
import org.apache.kafka.clients.consumer.{OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import scala.util.Random
import scala.concurrent.duration._


/**
  * Created by yxl on 16/12/9.
  */


class ClientAkkaConsumer extends Actor with ActorLogging {

    val recordsExt = ConsumerRecords.extractor[String, String]

    override def receive = LoggingReceive {
        case recordsExt(records) =>
            processRecords(records)
            sender() ! Confirm(records.offsets, commit = false)
        case _ => println("msg")
    }

    private def processRecords(records: ConsumerRecords[String, String]) = {
        records.pairs.foreach { case (key, value) =>
            log.info(s"Received [$key,$value]")
        }
        log.info(s"Batch complete, offsets: ${records.offsets}")
    }
}


object ClientAkkaConsumer {

    def main(args: Array[String]): Unit = {
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

        val system = ActorSystem()
        val actorConf = KafkaConsumerActor.Conf(1.seconds, 3.seconds)

        val receiveActor = system.actorOf(Props[ClientAkkaConsumer], name = "ClientAkkaConsumer")

        val consumer = system.actorOf(
            KafkaConsumerActor.props(consumerConfig, actorConf, receiveActor),
            name = "KafkaConsumerActor"
        )

        val topic = "structured-streaming-kafka-test"

        consumer ! AutoPartition(Seq(topic))

        // consumer ! Subscribe.ManualPartition(Seq( new TopicPartition(topic,0)))

    }

}
