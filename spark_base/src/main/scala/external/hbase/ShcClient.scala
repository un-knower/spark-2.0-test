package external.hbase

import java.util.concurrent.TimeUnit

import base.BaseSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by yxl on 2018/4/23.
  */
class ShcClient extends FunSuite with Matchers with BeforeAndAfter with BaseSession {

    override protected val appName: String = "ShcClient"

    import spark.implicits._

    val cat = s"""{
                 |"table":{"namespace":"default", "name":"test"},
                 |"rowkey":"key",
                 |"columns":{
                 |"id":{"cf":"rowkey", "col":"key", "type":"int"},
                 |"name":{"cf":"cf1", "col":"c1", "type":"string"},
                 |"age":{"cf":"cf1", "col":"c2", "type":"string"},
                 |"address":{"cf":"cf2", "col":"c1", "type":"string"}
                 |}
                 |}""".stripMargin

    test("init"){

        println(spark)

        val df = spark
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()

        println(df.count())
    }


    test("write"){

        val writeCat = s"""{
                     |"table":{"namespace":"default", "name":"shc_test"},
                     |"rowkey":"key",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"key", "type":"int"},
                     |"name":{"cf":"cf1", "col":"c1", "type":"string"},
                     |"age":{"cf":"cf1", "col":"c2", "type":"string"},
                     |"address":{"cf":"cf2", "col":"c1", "type":"string"}
                     |}
                     |}""".stripMargin


        import spark.implicits._

        val df = spark.sparkContext.parallelize(Seq((201,"value201"),(202,"value202"),(203,"value203")))
            .toDF("id","name")

        df.write
        .options(Map(HBaseTableCatalog.tableCatalog->writeCat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()


        val readDF = spark.read
        .options(Map(HBaseTableCatalog.tableCatalog->writeCat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()

        readDF.show()


    }

    test("schema"){
        val df = spark
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()

        df.printSchema()

        df.count()

        TimeUnit.SECONDS.sleep(1000000)
    }

}
