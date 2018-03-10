package yarn

import org.apache.spark.{Partitioner, SparkContext, SparkConf}

/**
 * Created by yxl on 16/6/6.
 */


case class FlightKey(letter: String, number: Int, sub: Double)

object FlightKey {
  implicit def orderingByIdAirportIdDelay[A <: FlightKey] : Ordering[A] = {
    Ordering.by(fk => (fk.letter, fk.number, fk.sub * -1))
  }
}

class AirlineFlightPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[FlightKey]
    println("letter:" + k.letter +  " hashcode:" +  k.letter.hashCode() + " partition:" +  k.letter.hashCode() % numPartitions)
    k.letter.hashCode() % numPartitions
  }
}

object TestSecondSortWithMulti {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestSecondSortWithMulti")
    .setMaster("local[2]")
    //.set("spark.cores.max", "1").set("spark.executor.memory", "128m")
    //.set("spark.driver.memory", "128m")
    .set("spark.local.dir", "/Users/yxl/data/spark.dir")


    val path = "/Users/yxl/data/test.csv"


    val sc = new SparkContext(sparkConf);


    //supporting code
    def createKeyValueTuple(data: Array[String]) :(FlightKey,List[String]) = {
      (createKey(data),listData(data))
    }

    def createKey(data: Array[String]): FlightKey = {
      FlightKey(data(0), (data(1)).toInt, (data(2)).toDouble)
    }

    def listData(data: Array[String]): List[String] = {
      List(data(2))
    }

    val rawDataArray = sc.textFile(path).map(line => line.split(","))

    val airlineData = rawDataArray.map(arr => createKeyValueTuple(arr))


    val keyedDataSorted = airlineData.repartitionAndSortWithinPartitions(new AirlineFlightPartitioner(2)).sortByKey() // shuffledRDD

    //only done locally for demo purposes, usually write out to HDFS
    keyedDataSorted.collect().foreach(println)


  }



}
