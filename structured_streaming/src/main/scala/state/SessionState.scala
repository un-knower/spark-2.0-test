package state

import java.util.concurrent.TimeUnit

import base.StructuredBase
import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}

import scala.util.Try

/**
  * nc -lk 9999
  *
  * session1,100
  * session2,100
  * session1,100,end
  */
object SessionState extends StructuredBase with Logging {
    override protected val appName: String = "SessionState"


    def main(args: Array[String]): Unit = {

        import spark.implicits._

        val socketDF = spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
        .as[String]


        val sessionUpdates = socketDF
        .filter(line => StringUtils.isNotEmpty(line))
        .map(line => {
            val columns = line.split(",")
            val endSignal = Try(Some(columns(2))).getOrElse(None)
            Session(columns(0), columns(1).toDouble, endSignal)
        })
        .groupByKey(_.sessionId)
        .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout)({
            case (sessionId: String, events: Iterator[Session], state: GroupState[SessionInfo]) => {
                val (iterator1, iterator2) = events.duplicate
                println(s"sessionId:${sessionId}, events:${iterator2.size}, state:${state}")
                val eventsSeq = iterator1.toSeq
                // 对groupByKey 后的数据进行处理,更新状态
                if (state.hasTimedOut) { // 如果超时
                    println(s"GroupState ${state} has timeout")
                    state.remove()
                    SessionUpdate(sessionId, -1, false) // 最终数据
                } else {
                    val sessionInfo = if (state.exists) {
                        val existingState = state.get
                        val updateSessionInfo = SessionInfo(existingState.totalSum
                        + eventsSeq.map(event => event.value).reduce(_ + _))
                        updateSessionInfo
                    } else {
                        println(s"create GroupState ${state}")
                        SessionInfo(eventsSeq.map(event => event.value).reduce(_ + _))
                    }

                    state.update(sessionInfo)
                    // 如果 10s 之内没有数据(下次batch 计算)，则删除state
                    state.setTimeoutDuration("10 seconds")

                    val isEndSignal = eventsSeq.filter(event => event.endSignal.isDefined).length > 0
                    if (isEndSignal) {
                        state.remove()
                        SessionUpdate(sessionId, sessionInfo.totalSum, true)
                    } else {
                        SessionUpdate(sessionId, sessionInfo.totalSum, false)
                    }
                }
            }
        })

        val query = sessionUpdates
        .writeStream
        .outputMode(OutputMode.Update())
        .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
        .format("console")
        .start()

        query.awaitTermination()

    }
}
