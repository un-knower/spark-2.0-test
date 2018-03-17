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
  * a,bike,2018-03-06 10:52:07
  * b,bike,2018-03-06 10:53:07
  * a,bike,2018-03-06 10:53:07
  *
  * 输出
  * a,bike,2018-03-06 10:52:07,2018-03-06 10:53:07
  */

object UserStateGroup extends StructuredBase {

    override protected val appName: String = "UserState"

    def main(args: Array[String]): Unit = {

        import spark.implicits._

        super.addListener(format = false)

        def updateUserState(userState: UserState, inputs: Iterator[InputRow]): UserState = {
            println(s"old user state:${userState}")
            for (input <- inputs) {
                if (input.timestamp.after(userState.end)) {
                    userState.end = input.timestamp
                }
                if (input.timestamp.before(userState.start)) {
                    userState.start = input.timestamp
                }
                // 标记
                if(!Seq("init","expire").contains(userState.activity)){
                    userState.activity = input.activity
                }
            }
            println(s"current user state:${userState}")
            userState
        }

        def updateAcrossEvents(user: String,
                               inputs: Iterator[InputRow],
                               state: GroupState[UserState]): UserState = {
            val userState: UserState = if (state.exists) {
                val oldUserState = state.get
                oldUserState.activity = "normal"
                oldUserState
            } else {
                println("create new user state !")
                UserState(user, "init", start = DateUtil.dateStr2Timestamp(DateUtil.getCurrent),
                    end = DateUtil.dateStr2Timestamp(DateUtil.START))
            }
            val newState = updateUserState(userState, inputs)
            // 更新state
            state.update(newState)
            newState
        }

        def updateAcrossEventsWithEventTime(user: String,
                                            inputs: Iterator[InputRow],
                                            state: GroupState[UserState]): UserState = {
            val userState = if (state.hasTimedOut) {
                state.remove()
                println("state expire !")
                val userState = UserState(user, "expire", start = DateUtil.dateStr2Timestamp(DateUtil.getCurrent),
                    end = DateUtil.dateStr2Timestamp(DateUtil.START))
                val newUserState = updateUserState(userState,inputs)
                state.update(newUserState)
                state.setTimeoutTimestamp(newUserState.end.getTime, "1 minute") // 如果超时，则设置EventTime 最大时间
                newUserState
            }else if(state.exists){
                val existsUserState= state.get
                existsUserState.activity = "normal"
                val newUserState = updateUserState(existsUserState,inputs)
                state.update(newUserState)
                newUserState
            }else{
                println("state init !")
                val userState = UserState(user, "init", start = DateUtil.dateStr2Timestamp(DateUtil.getCurrent),
                    end = DateUtil.dateStr2Timestamp(DateUtil.START))
                val newUserState = updateUserState(userState,inputs)
                val timeout = DateUtil.dateStr2Timestamp("2018-03-06 09:56:00")
                println(s"state timeout:${timeout}")
                state.setTimeoutTimestamp(timeout.getTime - 8 * 3600 * 1000, "1 minute") // 新创建，则设置EventTime 最小时间
                state.update(newUserState)
                newUserState
            }
            userState
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
            val dateTime = fmt.parseDateTime(array(2).trim)
            val timestamp = new Timestamp(dateTime.getMillis())
            InputRow(array(0), timestamp, array(1))
        })
        .withWatermark("timestamp", "1 minute")
        .groupByKey(_.user)

        // 没有设置过期时间
        // .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)

        // EventTime Timeout
        // 过期时间类型设置为EventTime时，需要声明watermark. EventTime 小于watermark设置的时间则被过滤掉。
        // 通过setTimeoutTimestamp()设置超时，当设置的时间小于watermark时发生超时
        .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(updateAcrossEventsWithEventTime)

        // ProcessingTime Timeout
        //.mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateAcrossEvents)


        val query = sessionUpdates.writeStream
        .queryName("UserStateGroup")
        .format("console")
        .outputMode(OutputMode.Update())
        .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
        .start()

        query.awaitTermination()


    }


}
