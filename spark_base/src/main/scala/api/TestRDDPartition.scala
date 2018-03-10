package api

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession

/**
  * Created by yxl on 2018/1/16.
  */
object TestRDDPartition {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
        .builder
        .appName("TestRDDPartition")
        .master("local[2]")
        .config("spark.local.dir", "/Users/yxl/data/spark.dir")
        .config("spark.default.parallelism",3)
        .getOrCreate()

        val base = "file:///Users/yxl/data/ml-1m/"

        // UserID::MovieID::Rating::Timestamp
        val data = spark.sparkContext.textFile(base + "ratings.dat")

        val ratings = data.map(line => {
             val array = line.split("::")
            (array(1).toInt,array(2).toDouble)
        })

        val movieRating = ratings.reduceByKey({
            case (x1,x2) => x1 + x2
        })

        val v = movieRating.reduce({
            case ((k1,v1),(k2,v2)) => {
                (k1 + k2, v1 + v2)
            }
        })

        println(v)

        TimeUnit.SECONDS.sleep(100000)

        spark.stop()


    }
}
