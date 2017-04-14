package client.producer

import cakesolutions.kafka.{KafkaProducerRecord, KafkaSerializer, KafkaProducer}
import org.apache.kafka.clients.producer.RecordMetadata
import concurrent.ExecutionContext.Implicits.global

import scala.util.{Try, Success}

/**
  * Created by yxl on 16/12/7.
  */
object ClientProducer {

    def main(args: Array[String]): Unit = {
        val serializer = (msg: String) => msg.getBytes
        val kafkaPort = 9092
        val producerConfig: KafkaProducer.Conf[String, String] = {
            KafkaProducer.Conf(KafkaSerializer(serializer),
                KafkaSerializer(serializer),
                bootstrapServers = s"localhost:$kafkaPort",
                acks = "all")
        }
        val topic = "state-direct-kafka"
        val producer = KafkaProducer(producerConfig)

        val message =
          """
            |{"msgid":3864045507,"data":{"event_type":31,"database":"beeper_tf","timestamp":"2017-04-14 15:15:28","storage":"mysql","values":{"comment":"","check_in_latitude":30.855371,"task_work_end_time":"1900-01-01 18:06:00","customer_tcsp":0,"task_work_begin_time":"1900-01-01 14:02:00","new_trade_level_one":1,"is_del":0,"task_is_back":0,"updated_at":"2017-04-14 15:03:28","fcc_id":5000573,"trans_task_id":1514274,"departure_latitude":30.855347,"driver_id":2304299,"is_late":0,"sales_id":1000745,"dprice_subsidy_total":1900,"sop_price_tax":92,"first_onboard_price":0,"id":5157822,"task_type":100,"check_in_ip":"","complete_time":"2017-04-13 14:02:10","cprice_per_day_tax":1140,"first_pay_money":0,"departure_ip":"182.144.78.219","car_team_id":0,"cargo_insurance_price":0,"departure_longitude":104.126951,"invoice_contents":"\u8d27\u7269\u8fd0\u8f93\u4ee3\u7406\u670d\u52a1\u8d39","dprice_per_day":19000,"task_warehouse_id":31767,"first_onboard_rate":0,"customer_id":11088,"type":100,"cprice_total":20520,"check_in_longitude":104.127,"task_onboard_date":"2017-04-13 00:12:00","status":900,"is_wrong_location":0,"cargo_insurance_rate":0,"invoice_tax_rate":0.06,"is_departure":1,"addition_count":0,"work_time":"2017-04-13 14:02:00","inspect_at":"2017-04-13 14:02:10","warehouse_id":31767,"task_line_name":"\u53cc\u6d41\u50f5\u5c38\u8f66\u56de\u6536+\u6295\u653e","is_addition":0,"addition_comment":"","sop_rate":0.08,"dd_id":1001182,"trans_driver_bid_id":3733477,"sop_royalty_price":380,"source_event_id":0,"cprice_per_day":19000,"car_num":"\u5dddJT9763","have_sop":1,"departure_time":"2017-04-13 13:01:56","cprice_total_with_tax":21752,"addition_seq":0,"day_off_refund_rate":0,"is_complete":1,"first_check_in":0,"sop_royalty_rate":0.25,"bid_mgr_id":1000874,"created_at":"2017-04-13 13:01:42","cprice_subsidy_total":0,"inspect_status":100,"cargo_insurance_price_tax":0,"exception_id":0,"check_in_time":"2017-04-13 13:01:42","task_schedule":"FREQ=DAILY;UNTIL=20170414T000000","day_off_refund_price":0,"adc_id":10,"is_supplement":0,"car_id":1,"sop_mgr_id":1000377,"have_temp_ctrl":0,"sop_price":1520},"table":"trans_event","type":"UPDATE","before":{"comment":"","check_in_latitude":30.855371,"task_work_end_time":"1900-01-01 18:06:00","customer_tcsp":0,"task_work_begin_time":"1900-01-01 14:02:00","new_trade_level_one":1,"is_del":0,"task_is_back":0,"updated_at":"2017-04-13 14:02:10","fcc_id":5000573,"trans_task_id":1514274,"departure_latitude":30.855347,"driver_id":2304299,"is_late":0,"sales_id":1000745,"dprice_subsidy_total":1900,"sop_price_tax":92,"first_onboard_price":0,"id":5157822,"task_type":100,"check_in_ip":"","complete_time":"2017-04-13 14:02:10","cprice_per_day_tax":1140,"first_pay_money":0,"departure_ip":"182.144.78.219","car_team_id":0,"cargo_insurance_price":0,"departure_longitude":104.126951,"invoice_contents":"\u8d27\u7269\u8fd0\u8f93\u4ee3\u7406\u670d\u52a1\u8d39","dprice_per_day":19000,"task_warehouse_id":31767,"first_onboard_rate":0,"customer_id":11088,"type":100,"cprice_total":20520,"check_in_longitude":104.127,"task_onboard_date":"2017-04-13 00:12:00","status":900,"is_wrong_location":0,"cargo_insurance_rate":0,"invoice_tax_rate":0.06,"is_departure":1,"addition_count":0,"work_time":"2017-04-13 14:02:00","inspect_at":"2017-04-13 14:02:10","warehouse_id":31767,"task_line_name":"\u53cc\u6d41\u50f5\u5c38\u8f66\u56de\u6536+\u6295\u653e","is_addition":0,"addition_comment":"","sop_rate":0.08,"dd_id":1001182,"trans_driver_bid_id":3733477,"sop_royalty_price":380,"source_event_id":0,"cprice_per_day":19000,"car_num":"\u5dddJT9763","have_sop":1,"departure_time":"2017-04-13 13:01:56","cprice_total_with_tax":21752,"addition_seq":0,"day_off_refund_rate":0,"is_complete":1,"first_check_in":0,"sop_royalty_rate":0.25,"bid_mgr_id":1000874,"created_at":"2017-04-13 13:01:42","cprice_subsidy_total":0,"inspect_status":100,"cargo_insurance_price_tax":0,"exception_id":0,"check_in_time":"2017-04-13 13:01:42","task_schedule":"FREQ=DAILY;UNTIL=20170414T000000","day_off_refund_price":0,"adc_id":10,"is_supplement":0,"car_id":1,"sop_mgr_id":1000377,"have_temp_ctrl":0,"sop_price":1520}}}
          """.stripMargin

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
