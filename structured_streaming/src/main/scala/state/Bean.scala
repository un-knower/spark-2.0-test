package state

import java.sql.Timestamp

import util.DateUtil

/**
  * Created by yxl on 2018/3/6.
  */

// 事件
case class Event(sessionId: String, timestamp: Timestamp)

// 保存到State中的信息
case class SessionInfo(
                      numEvents:Int,
                      startTimestamp:Timestamp,
                      endTimestamp:Timestamp
                      ){
    def durationMs: Long = startTimestamp.getTime - endTimestamp.getTime
}

// output console 输出
case class SessionUpdate(
                        id:String,
                        duration:Long,
                        numEvents:Int,
                        expired:Boolean
                        )

