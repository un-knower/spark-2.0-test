package common

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

/**
  * Created by yxl on 17/4/14.
  */

case class DateTime(year:String,month:String,day:String,hour:String,minute:String)

object DateUtil {

  val MINUTE_FORMAT = "yyyy-MM-dd_HH-mm"

  val SECONDS_FORMAT = "yyyy-MM-dd HH:mm:ss"

  val PERIOD_TEN = 600 * 1000

  def next(mills:Long,period:Long) = {
    (math.floor(mills.toDouble / period) + 1).toLong * period
  }

  def getNextTenMinute(mills:Long):String = {
    try{
      val nextMills = next(mills,PERIOD_TEN)
      val calendar = Calendar.getInstance()
      calendar.setTimeInMillis(nextMills)
      val sdf = new SimpleDateFormat(MINUTE_FORMAT)
      sdf.format(calendar.getTime())
    }catch{
      case ex:Exception => {
         null
      }
    }
  }

  def str2mills(str:String):Long = {
    try {
      val sdf = new SimpleDateFormat(SECONDS_FORMAT)
      sdf.parse(str).getTime
    }catch{
      case ex:Exception => {
        0l
      }
    }
  }


  def formatInt(value:Int):String = {
     if(value < 10) "0" + value
     else value.toString
  }

  def dateStr2DateTime(dateStr:String):DateTime = {
    val sdf = new SimpleDateFormat(MINUTE_FORMAT)
    val date = sdf.parse(dateStr)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    DateTime(formatInt(calendar.get(Calendar.YEAR)),
      formatInt(calendar.get(Calendar.MONTH)),
      formatInt(calendar.get(Calendar.DAY_OF_MONTH)),
      formatInt(calendar.get(Calendar.HOUR_OF_DAY)),
      formatInt(calendar.get(Calendar.MINUTE)))
  }

  def getCurrentMills:Long = {
      new Date().getTime
  }

}
