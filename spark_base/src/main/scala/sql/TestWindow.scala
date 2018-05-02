package sql

import base.BaseSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by yxl on 2018/4/4.
  */
object TestWindow extends BaseSession{
    override protected val appName: String = "TestWindow"

    def main(args: Array[String]): Unit = {

        val df = spark.createDataFrame(Seq((0,1,"2018-03-04"),(0,2,"2018-03-04"))).toDF("id","v","p_day")

        val  w1 = Window.partitionBy("id")

        df.withColumn("v2", mean(df.col("v")).over(w1)).show()

        val w2 = Window.partitionBy("id").orderBy("v")

        df.withColumn("v2", mean(df.col("v")).over(w2)).show()


        spark.sessionState.catalog

        val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t_test_2",Some("default")))

        val provider = catalogTable.provider.get

        spark.conf.set("hive.exec.dynamic.partition","true")
        spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

        df.write.mode(SaveMode.Append).format("hive").partitionBy("p_day").saveAsTable("t_test_2")

        df.write.mode(SaveMode.Append).format("parquet").partitionBy("p_day").saveAsTable("t_test_2")

        df.createOrReplaceTempView("spark_t_test_2")

        df.createOrReplaceTempView("t_test_2")

        spark.sql(
            """
              |create table if not exists default.t_test_2(
              |id int comment "ID",
              |v int comment "值"
              |) partitioned by (p_day string comment "分区日期")
              |stored as parquet
            """.stripMargin)

        spark.sql(
            """
              |create table if not exists default.t_test_2(
              | id int comment "id",
              | v  int comment "值",
              | p_day string comment "分区日期"
              |)
              |using parquet
              |partitioned by (p_day)
            """.stripMargin)

        spark.sql(
            """
              |insert overwrite table t_test_2 partition(p_day)
              |select id,v,p_day from spark_t_test_2
            """.stripMargin)

    }
}
