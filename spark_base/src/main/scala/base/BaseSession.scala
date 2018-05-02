package base

import org.apache.spark.sql.SparkSession

/**
  * Created by yxl on 2018/2/23.
  */
trait BaseSession {

    protected  val appName : String

    protected lazy val spark = SparkSession
    .builder
    .master("local[2]")
    .appName(appName)
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

}
