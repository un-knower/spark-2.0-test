import common.JsonUtil
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
  * Created by yxl on 17/4/14.
  */
class MessageSuite  extends FunSuite with Matchers with BeforeAndAfter {

  val message =
    """
      |{"msgid":3548386377,"data":{"event_type":31,"database":"beeper_tf","timestamp":"2017-04-14 14:07:51","storage":"mysql","values":{"comment":"","check_in_latitude":23.188822,"task_work_end_time":"1900-01-01 17:05:00","customer_tcsp":0,"task_work_begin_time":"1900-01-01 09:09:00","new_trade_level_one":2,"is_del":0,"task_is_back":0,"updated_at":"2017-04-14 14:02:51","fcc_id":5000554,"trans_task_id":1384119,"departure_latitude":23.181688,"driver_id":2157810,"is_late":0,"sales_id":1000143,"dprice_subsidy_total":2520,"sop_price_tax":154,"first_onboard_price":0,"id":4958547,"task_type":200,"check_in_ip":"","complete_time":null,"cprice_per_day_tax":3080,"first_pay_money":0,"departure_ip":"117.136.41.83","car_team_id":0,"cargo_insurance_price":300,"departure_longitude":113.661279,"invoice_contents":"\u8fd0\u8d39","dprice_per_day":28000,"task_warehouse_id":24003,"first_onboard_rate":0,"customer_id":9523,"type":100,"cprice_total":29700,"check_in_longitude":113.659501,"task_onboard_date":"2016-12-14 00:12:00","status":800,"is_wrong_location":0,"cargo_insurance_rate":0,"invoice_tax_rate":0.11,"is_departure":1,"addition_count":0,"work_time":"2017-04-14 09:09:00","inspect_at":null,"warehouse_id":24003,"task_line_name":"\u9ec4\u57d4\u2014\u5f00\u53d1\u533a","is_addition":0,"addition_comment":"","sop_rate":0.05,"dd_id":1001205,"trans_driver_bid_id":3175354,"sop_royalty_price":350,"source_event_id":0,"cprice_per_day":28000,"car_num":"\u7ca4A0MQ04","have_sop":1,"departure_time":"2017-04-14 14:02:51","cprice_total_with_tax":32967,"addition_seq":0,"day_off_refund_rate":0,"is_complete":0,"first_check_in":0,"sop_royalty_rate":0.25,"bid_mgr_id":1001406,"created_at":"2017-03-16 00:12:42","cprice_subsidy_total":0,"inspect_status":100,"cargo_insurance_price_tax":33,"exception_id":0,"check_in_time":"2017-04-14 07:07:15","task_schedule":"FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR,SA,SU","day_off_refund_price":0,"adc_id":3,"is_supplement":0,"car_id":5,"sop_mgr_id":1001297,"have_temp_ctrl":0,"sop_price":1400},"table":"trans_event","type":"UPDATE","before":{"comment":"","check_in_latitude":23.188822,"task_work_end_time":"1900-01-01 17:05:00","customer_tcsp":0,"task_work_begin_time":"1900-01-01 09:09:00","new_trade_level_one":2,"is_del":0,"task_is_back":0,"updated_at":"2017-04-14 07:07:15","fcc_id":5000554,"trans_task_id":1384119,"departure_latitude":0,"driver_id":2157810,"is_late":0,"sales_id":1000143,"dprice_subsidy_total":2520,"sop_price_tax":154,"first_onboard_price":0,"id":4958547,"task_type":200,"check_in_ip":"","complete_time":null,"cprice_per_day_tax":3080,"first_pay_money":0,"departure_ip":"","car_team_id":0,"cargo_insurance_price":300,"departure_longitude":0,"invoice_contents":"\u8fd0\u8d39","dprice_per_day":28000,"task_warehouse_id":24003,"first_onboard_rate":0,"customer_id":9523,"type":100,"cprice_total":29700,"check_in_longitude":113.659501,"task_onboard_date":"2016-12-14 00:12:00","status":400,"is_wrong_location":0,"cargo_insurance_rate":0,"invoice_tax_rate":0.11,"is_departure":0,"addition_count":0,"work_time":"2017-04-14 09:09:00","inspect_at":null,"warehouse_id":24003,"task_line_name":"\u9ec4\u57d4\u2014\u5f00\u53d1\u533a","is_addition":0,"addition_comment":"","sop_rate":0.05,"dd_id":1001205,"trans_driver_bid_id":3175354,"sop_royalty_price":350,"source_event_id":0,"cprice_per_day":28000,"car_num":"\u7ca4A0MQ04","have_sop":1,"departure_time":null,"cprice_total_with_tax":32967,"addition_seq":0,"day_off_refund_rate":0,"is_complete":0,"first_check_in":0,"sop_royalty_rate":0.25,"bid_mgr_id":1001406,"created_at":"2017-03-16 00:12:42","cprice_subsidy_total":0,"inspect_status":100,"cargo_insurance_price_tax":33,"exception_id":0,"check_in_time":"2017-04-14 07:07:15","task_schedule":"FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR,SA,SU","day_off_refund_price":0,"adc_id":3,"is_supplement":0,"car_id":5,"sop_mgr_id":1001297,"have_temp_ctrl":0,"sop_price":1400}}}
    """.stripMargin

  test("json read") {

    val map = JsonUtil.toMap[Object](message)
    val data = map.get("data")
    val msgId = map.getOrElse("msgid","0")
    val record = data match {
      case None => None
      case Some(eventMap) => {
          val eventDataMap = eventMap.asInstanceOf[Map[String,Object]]
          val timestamp = eventDataMap.getOrElse("timestamp","0")
          val eventType = eventDataMap.getOrElse("type","-")
          val values = eventDataMap.get("values")
          values match {
            case None => None
            case Some(valueMap) => {
               val eventValueMap = valueMap.asInstanceOf[Map[String,Object]]
               val driverId = eventValueMap.getOrElse("driver_id","0")
               val adcId = eventValueMap.getOrElse("adc_id","0")
               val status = eventValueMap.getOrElse("status","0")
               val eventPrice = eventValueMap.getOrElse("cprice_per_day","0")
              Some(msgId,timestamp,eventType,driverId,adcId,status,eventPrice)
            }
          }
      }
    }
    println(record)

  }


}
