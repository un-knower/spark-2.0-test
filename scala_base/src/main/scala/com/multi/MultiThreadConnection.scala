package com.multi

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import com.util.DBUtil

import scala.collection.mutable

/**
  * Created by yxl on 2018/3/13.
  */


case class Score(userId: Int, score: Double)

object MultiThreadConnection {

    def main(args: Array[String]): Unit = {

        val size = 20
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "t_score"

        val scores = Seq(
            Score(1, 1.0),
            Score(1, 2.0),
            Score(2, 3.0),
            Score(2, 4.0),
            Score(3, 2.0),
            Score(3, 2.0),
            Score(1, 2.0),
            Score(1, 2.0),
            Score(1, 1.0),
            Score(1, 2.0),
            Score(2, 3.0),
            Score(2, 4.0),
            Score(3, 2.0)
        )

        class MyUncaughtExceptionHandler extends Thread.UncaughtExceptionHandler {
            override def uncaughtException(t: Thread, e: Throwable): Unit = {
                System.out.println("\n [caugth:] " + e.printStackTrace())
            }
        }

        Thread.setDefaultUncaughtExceptionHandler(new MyUncaughtExceptionHandler)

        val executors = Executors.newFixedThreadPool(size + 10)


        val countDownLatch = new CountDownLatch(size)

        class Work(latch:CountDownLatch) extends Runnable {

            override def run(): Unit = {
                latch.await()
                val connection = DBUtil.createMySQLConnectionFactory(url, "root", "root")
                scores.map(score => {
                    val querySQL =
                        s"""
                           | select user_id from $table where user_id = ?
                            """.stripMargin
                    val count = DBUtil.runQuerySQLCount(connection, querySQL, Seq(score.userId))
                    if (count == 0) {
                        val columnMap = new mutable.HashMap[String, Any]()
                        columnMap.put("user_id", score.userId)
                        columnMap.put("score", score.score)
                        DBUtil.insertTable(connection, columnMap.toMap[String, Any], table)
                    } else {
                        val sql =
                            s"""
                               |update $table
                               |  set
                               |     user_id = ? ,
                               |     score = ?
                               |where user_id = ?
                                """.stripMargin
                        DBUtil.runSQL(connection, sql, Seq(score.userId, score.score, score.userId))
                    }
                })
            }
        }

        (1 to size).map(index => {
            println(s"start thread index : ${index}")
            countDownLatch.countDown()
            executors.execute(new Work(countDownLatch))
        })

        executors.shutdown()
    }

}
