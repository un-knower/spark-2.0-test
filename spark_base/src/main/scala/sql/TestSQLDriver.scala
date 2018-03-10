package sql

import base.BaseSession

/**
  * Created by yxl on 2018/2/23.
  */
object TestSQLDriver extends BaseSession {
    override protected val appName: String = "TestSQLDriver"

    def main(args: Array[String]): Unit = {

        val sqlContext = spark.sqlContext

        val sql = ""

        spark.sparkContext.setJobDescription(sql)

    }
}
