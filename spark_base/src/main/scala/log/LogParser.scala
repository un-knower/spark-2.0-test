package log

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._


/**
 * @author ChenCheng
 */


object mysqltohdfs {
  def main (args:Array[String]){
    val sparkConf = new SparkConf().setAppName("mysqltohdfs").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val regex = """.*"time":"([0-9- :]+)".*"userId":"([0-9]+)".*"os":"([a-zA-Z0-9]+)".*"channel":"([a-zA-Z0-9]+)".*""".r
    val lists = List("API_BID:","API_REG:","API_LOGIN:","API_PAY_APPLY:","API_CASHOUT_APPLY:")
    //val buf = new ArrayBuffer[String]
    //class Person (userId:Int,time:String,channel:String)
    sc.textFile("/Users/yxl/data/log.txt").map(s=>{
      var temp = false
      for(list<-lists ;if temp == false; if s.contains(list))
      yield
      { temp = true
        s.split(list)(1)
      }
    }).map(s=>{s.mkString("") match {
      case regex(time,userId,os,channel) => (userId.trim.toInt,(time,os+'-'+channel))
      case _ => (-1,(null,null))
    }}).filter(s=> s._1.toInt != -1)
      .reduceByKey((x,y)=>if (x._1.compareTo(y._1) >0) x else y )
     .foreach(print(_))
    //.reduceByKey((x,y)=>x._1<y._1)
  }
}