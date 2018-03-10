package base

import org.apache.spark.sql.SparkSession

/**
  * Created by yxl on 2018/3/2.
  */
trait StructuredBase {

    protected val appName :String

    protected lazy val spark = SparkSession
    .builder()
    .appName(appName)
    .master("local[2]")
    .config("spark.sql.shuffle.partitions",2)
    .config("spark.local.dir","/Users/yxl/data/spark")
    .config("spark.shuffle.compress", false)
    .getOrCreate()



}
