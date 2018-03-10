package com.example

import java.util.concurrent.TimeUnit

import scala.util.Random

/**
  * Created by yxl on 2018/1/16.
  */
abstract class Closed {

    def init() = {
        val thread = new Thread(new Runnable {
            override def run(): Unit = {
                println("启动监控线程....")
                while (true) {
                    TimeUnit.SECONDS.sleep(5)
                    val random = Random.nextInt(100)
                    println(s"随机数:$random")
                    if (random == 5) {
                        println("停止")
                        stop()
                        return
                    }
                }
            }
        })

        thread.start()
    }

    def stop()

}
