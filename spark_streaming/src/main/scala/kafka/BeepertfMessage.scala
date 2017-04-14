package kafka

import _root_.common.JsonUtil

/**
  * Created by yxl on 17/4/14.
  */
object BeepertfMessage {

  def parseMessage(message:String) : Option[TransEventMessage] = {
    val map = JsonUtil.toMap[Object](message)
    val data = map.get("data")
    val msgId = map.getOrElse("msgid", "0").toString
    val record = data match {
      case None => None
      case Some(eventMap) => {
        val eventDataMap = eventMap.asInstanceOf[Map[String, Object]]
        val timestamp = eventDataMap.getOrElse("timestamp", "0").toString
        val eventType = eventDataMap.getOrElse("type", "-").toString
        val values = eventDataMap.get("values")
        values match {
          case None => None
          case Some(valueMap) => {
            val eventValueMap = valueMap.asInstanceOf[Map[String, Object]]
            val driverId = eventValueMap.getOrElse("driver_id", "0").toString
            val adcId = eventValueMap.getOrElse("adc_id", "0").toString
            val status = eventValueMap.getOrElse("status", "0").toString
            val eventPrice = eventValueMap.getOrElse("cprice_per_day", "0").toString
            Some(TransEventMessage(msgId, timestamp, eventType, driverId, adcId, status, eventPrice))
          }
        }
      }
    }
    record
  }

}
