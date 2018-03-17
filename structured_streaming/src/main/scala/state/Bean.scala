package state

import java.sql.Timestamp

import util.DateUtil

/**
  * Created by yxl on 2018/3/6.
  */

case class InputRow(user: String, timestamp: Timestamp, activity: String)


case class UserState(user: String,
                     var activity: String,
                     var start: Timestamp,
                     var end: Timestamp)

// 每个group 的 schema
case class Session(sessionId:String,value:Double,endSignal:Option[String])

// 保存到 state 中数据
case class SessionInfo(totalSum:Double)

// 汇总状态
case class SessionUpdate(id:String,totalSum:Double,expired:Boolean)