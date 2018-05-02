package external.hbase

import base.BaseSession
import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by yxl on 2018/4/24.
  */
class SparkClient extends FunSuite with Matchers with BeforeAndAfter with BaseSession {
    override protected val appName: String = "SparkClient"


    test("hbase format read"){
        val hbaseConfiguration = HBaseConfiguration.create()
        hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM,"localhost:2181")
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE,"test")

        hbaseConfiguration.setInt(TableInputFormat.SCAN_ROW_START,1)
        hbaseConfiguration.setInt(TableInputFormat.SCAN_ROW_STOP, 50)

        val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConfiguration,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        ).map(x => x._2)


        val count = hbaseRDD.count()
        println(count)

        hbaseRDD.foreach(x => println(RowKey.get(x.getRow)))

//        val results = hbaseRDD.take(10)
//
//        results.foreach(x => {
//            val row = Bytes.toString(x.getRow)
//            println(row)
//        })
    }


    test("hbase format write"){
        val hbaseConfiguration = HBaseConfiguration.create()
        hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM,"localhost:2181")
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE,"test")

        val job = Job.getInstance(hbaseConfiguration)

        val configuration = job.getConfiguration()
        configuration.set(TableOutputFormat.OUTPUT_TABLE, "test")

        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        spark.sparkContext.parallelize(Seq(101,102,103))
        .map(x =>{
            val cf1 = Bytes.toBytes("cf1")
            val cf2 = Bytes.toBytes("cf2")
            val qualifier1 = Bytes.toBytes("c1")
            val qualifier2 = Bytes.toBytes("c2")
            val value = Bytes.toBytes("value" + x)
            val put = new Put(RowKey.hash(x.toString))
            put.addColumn(cf1, qualifier1, value)
            put.addColumn(cf2, qualifier1, value)
            put.addColumn(cf1, qualifier2, value)
            put.addColumn(cf2, qualifier2, value)
            (new ImmutableBytesWritable(),put)
        })
        .saveAsNewAPIHadoopDataset(configuration)

    }

    test("hbase hfile kv"){

        val  data = spark.sparkContext.parallelize(Seq(
            (4,"d",17,0),
            (5,"e",15,0),
            (6,"f",20,1)
        )).map(x => {
            (x._1,x)
        })
        .sortByKey(true) // row 有序
        .map(x =>{
            val id = x._2._1
            val name = x._2._2
            val age = x._2._3
            val status = x._2._4

            val kv1 = new KeyValue(Bytes.toBytes(id),Bytes.toBytes("cf1"),Bytes.toBytes("age"), Bytes.toBytes(age))
            val kv2 = new KeyValue(Bytes.toBytes(id),Bytes.toBytes("cf1"),Bytes.toBytes("name"), Bytes.toBytes(name))
            val kv3 = new KeyValue(Bytes.toBytes(id),Bytes.toBytes("cf1"),Bytes.toBytes("status"), Bytes.toBytes(status))
            (id,Seq(kv1,kv2,kv3)) // columnFamily:qualifier 有序

        })
        .flatMap(x =>{
            val kvs = x._2
            kvs.map(i =>{
                (new ImmutableBytesWritable(),i)
            })
        })

        val pathStr = "/tmp/shc_test/hfile"
        val path = new Path(pathStr)
        val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
        val fileSystem = path.getFileSystem(hadoopConfiguration)
        fileSystem.delete(path,true)

        val hbaseConfiguration = HBaseConfiguration.create()
        hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM,"localhost:2181")
        val connection = ConnectionFactory.createConnection(hbaseConfiguration)
        val tableName = TableName.valueOf("shc_test")
        val admin = connection.getAdmin()
        val htable = connection.getTable(tableName).asInstanceOf[HTable]
        val regionLocator = connection.getRegionLocator(tableName)

        // 保存到HDFS 上,再进行load
        data.saveAsNewAPIHadoopFile(pathStr,classOf[ImmutableBytesWritable],
            classOf[KeyValue],classOf[HFileOutputFormat2])
        val bulkLoader = new LoadIncrementalHFiles(hbaseConfiguration)
        bulkLoader.doBulkLoad(path,admin,htable,regionLocator)

        //直接写HFile到table中(并不好用)
//        val job = Job.getInstance(hbaseConfiguration)
//        HFileOutputFormat2.configureIncrementalLoad(job,htable,regionLocator)
//        data.saveAsNewAPIHadoopFile(pathStr,classOf[ImmutableBytesWritable],
//            classOf[KeyValue],classOf[HFileOutputFormat2],job.getConfiguration)
    }

}
