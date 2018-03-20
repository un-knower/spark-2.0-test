package util

import java.sql.Timestamp

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/**
  * Created by yxl on 2018/3/10.
  */
object DateUtil {

    val START = "1970-01-01 00:00:00"

    val CURRENT = new DateTime().toString("yyyy-MM-dd HH:mm:ss")

    /**
      * @param str  yyyy-MM-dd HH:mm:ss
      * @return
      */
    def dateStr2Timestamp(str:String):Timestamp = {
        val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        val dateTime = fmt.parseDateTime(str.trim)
        val timestamp = new Timestamp(dateTime.getMillis())
        timestamp
    }


    def mills2Timestamp(mills:Long):Timestamp = {
        val timestamp = new Timestamp(mills)
        timestamp
    }

    /**
      * 获取当前日期
      * @return
      */
    def getCurrent = {
        new DateTime().toString("yyyy-MM-dd HH:mm:ss")
    }

}
