package com.multi

import com.util.DBUtil

/**
  * Created by yxl on 2018/3/14.
  */
object Test {
    def main(args: Array[String]): Unit = {
        val url = "jdbc:mysql://localhost:3306/test"
        val connection = DBUtil.createMySQLConnectionFactory(url, "root","root")
        println(connection)
    }
}
