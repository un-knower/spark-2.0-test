package base

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import util.DateUtil

/**
  * Created by yxl on 2018/3/2.
  */
trait StructuredBase {

    protected val appName :String

    protected  val topic = "structured-streaming-kafka-test"

    protected val checkpointPath:String = "file:///Users/yxl/data/spark/checkpoint"

    protected lazy val spark = getOrCreateSpark

    protected  def getOrCreateSpark = {
        val spark = SparkSession
        .builder()
        .appName(appName)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.local.dir", "/Users/yxl/data/spark")
        .config("spark.shuffle.compress", false)
        .getOrCreate()

        spark
    }

    protected def submitSpark = {
        val spark = SparkSession
        .builder()
        .appName(appName)
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.shuffle.compress", false)
        .getOrCreate()
        spark
    }

    protected def socketData = {

        import spark.implicits._

        val socketData = spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
        .as[String]

        socketData
    }

    protected def kafkaData(topic:String,startOffset:String = "latest") = {
        import spark.implicits._

        val kafkaData = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", startOffset)
        .load()
        .select("value") // 只获取value 字段
        .as[String]

        kafkaData
    }

    def addListener(format:Boolean = false): Unit ={
        spark.streams.addListener(new StreamingQueryListener() {
            override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
                println("Query started: " + queryStarted.id)
            }
            override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
                println("Query terminated: " + queryTerminated.id)
            }
            override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
                if(format){
                    val current = DateUtil.getCurrent
                    val progress = queryProgress.progress
                    val batchId = progress.batchId
                    val numInputRows = progress.numInputRows
                    val eventTime = progress.eventTime
                    val maxEventTime = eventTime.getOrDefault("max",null)
                    val watermark = eventTime.getOrDefault("watermark",null)
                    val string = s"batchId:$batchId, numInputRows:$numInputRows, " +
                    s"maxEventTime:${maxEventTime}, watermark:${watermark}, current:${current}"
                    println(string)

                }else{
                    println("Query made progress: " + queryProgress.progress)
                }
            }
        })
    }

    def consoleShow(dataFrame:DataFrame,duration:Int, mode:OutputMode, checkpoint:Boolean= true): StreamingQuery ={
        val writeStream = dataFrame.writeStream
        .outputMode(mode)
        .trigger(Trigger.ProcessingTime(duration, TimeUnit.SECONDS))
        if(checkpoint){
            writeStream.option("checkpointLocation", this.checkpointPath +  "/" + this.appName)
        }
        .format("console")
        .queryName(this.appName)

        val query = writeStream.start()
        query
    }

    def foreachShow(dataFrame: DataFrame,duration:Int, mode:OutputMode, checkpoint:Boolean = true):StreamingQuery = {
        val writeStream = dataFrame.writeStream
        .foreach(new ForeachWriter[Row] {
            override def process(value: Row): Unit = {
                println(s"process:$value")
            }

            override def close(errorOrNull: Throwable): Unit = {
                // println("close ...")
            }

            override def open(partitionId: Long, version: Long): Boolean = {
                //println(s"open partitionId:$partitionId  version:$version")
                true
            }
        })
        .outputMode(mode)
        .trigger(Trigger.ProcessingTime(duration,TimeUnit.SECONDS))
        .queryName(this.appName)

        if(checkpoint){
            writeStream.option("checkpointLocation", this.checkpointPath +  "/" + this.appName)
        }

        val query = writeStream.start()

        query
    }


}
