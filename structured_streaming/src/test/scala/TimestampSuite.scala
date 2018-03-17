import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.catalyst.expressions.CurrentBatchTimestamp
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataTypes, TimestampType}
import org.apache.spark.unsafe.types.CalendarInterval
import org.joda.time.Months
import org.joda.time.format.DateTimeFormat
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by yxl on 17/7/3.
  */
class TimestampSuite  extends FunSuite with Matchers with BeforeAndAfter {


    test("timestamp"){
        val ms = new Date().getTime()
        val ts = CurrentBatchTimestamp(ms.toLong,TimestampType)
        println(ts)
        DataTypes.TimestampType
    }

    test("timestamp1"){
        val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        val dateTime = fmt.parseDateTime("2018-03-05 09:20:07")
        println(dateTime.getMillis())
        val timestamp = new Timestamp(dateTime.getMillis())
        println(timestamp.getTime)
        val sqlTimestamp = DateTimeUtils.fromJavaTimestamp(timestamp)
        println(sqlTimestamp)
        // 1461756862000
        // 1520212807000

    }

    test("format interval"){
        val delayThreshold = "1 minute"
        val interval = CalendarInterval.fromString("interval " + delayThreshold)
        println(interval.microseconds)
    }


    test("duration"){
        val sdf = new SimpleDateFormat("yyyy-MM")
        val startCalendar = Calendar.getInstance
        val endCalendar = Calendar.getInstance

        startCalendar.setTime(sdf.parse("2017-01"))
        endCalendar.setTime(sdf.parse("2016-03"))

        val result = endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH)
        val month = (endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR)) * 12

        val months = month + result

        println(months)

    }

}
