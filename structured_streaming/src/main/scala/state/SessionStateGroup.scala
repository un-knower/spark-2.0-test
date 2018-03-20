package state

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import base.StructuredBase
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.joda.time.format.DateTimeFormat
import util.DateUtil

/**
  * nc -lk 9999
  * a,2018-03-06 10:52:07
  * b,2018-03-06 10:53:07
  * a,2018-03-06 10:53:07
  *
  * 输出
  * a,2018-03-06 10:52:07,2018-03-06 10:53:07
  */

object SessionStateGroup extends StructuredBase {

    override protected val appName: String = "SessionStateGroup"

    def main(args: Array[String]): Unit = {

        import spark.implicits._

        super.addListener(format = true)

        // no time out
        def updateSessionState(sessionId: String,
                               events: Iterator[Event],
                               state: GroupState[SessionInfo]): SessionUpdate = {
            val timestamps = events.map(_.timestamp.getTime).toSeq

            val updateSessionInfo = if (state.exists) { // 更新
                println(s"state exists !")
                val oldSessionInfo = state.get
                SessionInfo(
                    oldSessionInfo.numEvents + timestamps.size,
                    oldSessionInfo.startTimestamp,
                    DateUtil.mills2Timestamp(Math.max(oldSessionInfo.endTimestamp.getTime, timestamps.max))
                )
            } else { // 创建
                println("state init !")
                SessionInfo(
                    timestamps.size,
                    DateUtil.mills2Timestamp(timestamps.min),
                    DateUtil.mills2Timestamp(timestamps.max)
                )
            }
            state.update(updateSessionInfo)
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
        }

        // event time out
        def updateSessionStateWithEventTime(sessionId: String,
                                                 events: Iterator[Event],
                                                 state: GroupState[SessionInfo]): SessionUpdate = {

            if (state.hasTimedOut) {
                println("state expire !")
                val finalSessionUpdate = SessionUpdate(
                    sessionId,
                    state.get.durationMs,
                    state.get.numEvents,
                    expired = true
                )
                state.remove()
                finalSessionUpdate
            } else {
                val timestamps = events.map(_.timestamp.getTime).toSeq

                val updateSessionInfo = if (state.exists) { // 更新
                    println(s"state exists !")
                    val oldSessionInfo = state.get
                    SessionInfo(
                        oldSessionInfo.numEvents + timestamps.size,
                        oldSessionInfo.startTimestamp,
                        DateUtil.mills2Timestamp(Math.max(oldSessionInfo.endTimestamp.getTime, timestamps.max))
                    )
                } else { // 创建
                    println("state init !")
                    SessionInfo(
                        timestamps.size,
                        DateUtil.mills2Timestamp(timestamps.min),
                        DateUtil.mills2Timestamp(timestamps.max)
                    )
                }
                state.update(updateSessionInfo)
                state.setTimeoutTimestamp(DateUtil.mills2Timestamp(timestamps.max).getTime)
                SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
            }

        }


        // process time out
        def updateSessionStateWithProcessingTime(sessionId: String,
                                                 events: Iterator[Event],
                                                 state: GroupState[SessionInfo]): SessionUpdate = {

            if (state.hasTimedOut) { // 超时的State 依然可以获取，需要手动处理超时情况
                println("state expire !")
                val finalSessionUpdate = SessionUpdate(
                    sessionId,
                    state.get.durationMs,
                    state.get.numEvents,
                    expired = true
                )
                state.remove()
                finalSessionUpdate
            } else {
                val timestamps = events.map(_.timestamp.getTime).toSeq

                val updateSessionInfo = if (state.exists) { // 更新
                    println(s"state exists !")
                    val oldSessionInfo = state.get
                    SessionInfo(
                        oldSessionInfo.numEvents + timestamps.size,
                        oldSessionInfo.startTimestamp,
                        DateUtil.mills2Timestamp(Math.max(oldSessionInfo.endTimestamp.getTime, timestamps.max))
                    )
                } else { // 创建
                    println("state init !")
                    SessionInfo(
                        timestamps.size,
                        DateUtil.mills2Timestamp(timestamps.min),
                        DateUtil.mills2Timestamp(timestamps.max)
                    )
                }
                state.update(updateSessionInfo)
                // 如果10s 没有收到数据，则state 过期
                state.setTimeoutDuration("10 seconds")
                SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
            }

        }

        val lines = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
        .as[String]

        val sessionUpdates = lines.filter(line => {
            StringUtils.isNotEmpty(line)
        })
        .map(line => { // user, timestamp, activity
            val array = line.split(",")
            val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            val dateTime = fmt.parseDateTime(array(1).trim)
            val timestamp = new Timestamp(dateTime.getMillis())
            Event(array(0), timestamp)
        })
        .withWatermark("timestamp", "1 minute")
        .groupByKey(_.sessionId)

        /**
          * mapGroupsWithState 需要三个参数 ：
          *  - group 的 key 可以理解为state的标识
          *  - 一次trigger 中包含的数据
          *  - 用户定义的State对象，用来保存状态
          */

        // 没有设置过期时间
        // .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateSessionState)

        // 过期时间类型设置为EventTime时,需要声明watermark. EventTime 小于watermark的被过滤掉。
        // 通过setTimeoutTimestamp()设置超时时间，当watermark大于设置的时间则发生超时
        // 设置的超时时间不能小于watermark的时间
        .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(updateSessionStateWithEventTime)


        // ProcessingTime Timeout (system clock)
        // 不需要设置watermark
        // 通过setTimeoutDuration() 设置超时时间，是按照处理时间(trigger 执行时间)和设置的duration来判断超时是否发生,只有当trigger有数据时才会判断
        // 如果trigger 的data 中包含某个group key 的数据,则即使过了超时时间，也不超时
        //.mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout())(updateSessionStateWithProcessingTime)

        val query = sessionUpdates.writeStream
        .queryName("SessionStateGroup")
        .format("console")
        .outputMode(OutputMode.Update())
        .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
        .start()

        query.awaitTermination()


    }


}
