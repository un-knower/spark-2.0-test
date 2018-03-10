package state

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import base.StructuredBase
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.joda.time.format.DateTimeFormat

/**
  *  nc -lk 9999
  *  a,bike,2018-03-06 10:52:07
  *  b,bike,2018-03-06 10:53:07
  *  a,bike,2018-03-06 10:53:07
  */

object UserStateGroup extends StructuredBase {

    override protected val appName: String = "UserState"

    def main(args: Array[String]): Unit = {

        import spark.implicits._

        //根据给定的InputRow 更新 UserState
        def updateUserStateWithEvent(state: UserState, input: InputRow): UserState = {
            if (Option(input.timestamp).isEmpty) {
                return state
            }
            //update exists
            if (state.activity == input.activity) {
                if (input.timestamp.after(state.end)) {
                    state.end = input.timestamp
                }
                if (input.timestamp.before(state.start)) {
                    state.start = input.timestamp
                }
            } else {
                //create new
                if (input.timestamp.after(state.end)) {
                    state.start = input.timestamp
                    state.end = input.timestamp
                    state.activity = input.activity
                }
            }
            //return the updated state
            state
        }


        def updateAcrossEvents(user: String,
                               inputs: Iterator[InputRow],
                               oldState: GroupState[UserState]): UserState = {
            var state: UserState = if (oldState.exists) oldState.get else UserState(user,
                "",
                new Timestamp(6284160000000L),
                new Timestamp(6284160L)
            )
            // we simply specify an old date that we can compare against and
            // immediately update based on the values in our data

            for (input <- inputs) {
                state = updateUserStateWithEvent(state, input)
                oldState.update(state)
            }
            state
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
        .groupByKey(_.user)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)


        val query = sessionUpdates.writeStream
        .queryName("UserStateGroup")
        .format("console")
        .outputMode(OutputMode.Update())
        .trigger(Trigger.ProcessingTime(10,TimeUnit.SECONDS))
        .start()

        query.awaitTermination()


    }


}
