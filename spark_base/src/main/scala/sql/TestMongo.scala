package sql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.Document
import com.mongodb.spark.sql._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}

/**
  * Created by yxl on 16/9/21.
  */

case class Movie(id: Int, title: String, genres: String)

case class User(id: Int, gender: String, age: String, occupation: String, zip: String)


class TestMongo extends FunSuite with BeforeAndAfterAll {

  var sparkContext: SparkContext = _

  var sqlContext: SQLContext = _


  override protected def beforeAll(): Unit = {

    val sparkConf = new SparkConf().setAppName("WriteMongo")
      .setMaster("local[3]")
      .set("spark.cores.max", "3").set("spark.executor.memory", "1024m")
      .set("spark.driver.memory", "128m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    sparkContext = new SparkContext(sparkConf)

    sqlContext = SQLContext.getOrCreate(sparkContext)


  }

  test("read from mongo ratings") {
    val config = ReadConfig(Map(
      "spark.mongodb.input.uri" -> "mongodb://192.168.0.42:20001",
      "spark.mongodb.input.database" -> "beeper2",
      "spark.mongodb.input.collection" -> "to_driver_evaluations",
      "spark.mongodb.input.partitioner" -> "MongoSinglePartitioner"
    ))


    val drivers = MongoSpark.load(sqlContext , config)

    drivers.write.mode(SaveMode.Overwrite).saveAsTable("t_evaluations")

    val df = sqlContext.sql("select cuid,dstring,day_score,sum_score from ( " +
      "select cuid,dstring,day_score,sum(day_score) over (partition by cuid order by dstring asc) as sum_score" +
      " from ( select cuid , sum(score) as day_score , to_date(cts) as dstring " +
      "from t_evaluations group by cuid,to_date(cts) ) t1 ) t2" +
      " where dstring >='2016-07-14' and dstring < '2016-08-15' limit 100")

    println(drivers.count())

  }


  test("read from mongo users write to mongo") {

    val usersReadConfig = ReadConfig(Map(
      "spark.mongodb.input.uri" -> "mongodb://127.0.0.1",
      "spark.mongodb.input.database" -> "recommend",
      "spark.mongodb.input.collection" -> "ratings",
      "spark.mongodb.input.partitioner" -> "MongoSplitVectorPartitioner"
    ))

    val users = MongoSpark.load(sqlContext, usersReadConfig)

    MongoSpark.save(users.write.options(
      Map("spark.mongodb.output.uri" -> "mongodb://linux1",
        "spark.mongodb.output.database" -> "recommend",
        "spark.mongodb.output.collection" -> "users")).mode(SaveMode.Overwrite))

  }

  test("read from mongo movies save to hive") {

    val movieReadConfig = ReadConfig(Map(
      "spark.mongodb.input.uri" -> "mongodb://127.0.0.1",
      "spark.mongodb.input.database" -> "recommend",
      "spark.mongodb.input.collection" -> "movies",
      "spark.mongodb.input.partitioner" -> "MongoSplitVectorPartitioner"
    ))

    val movies = MongoSpark.load(sqlContext, movieReadConfig)

    //movies.printSchema()

    val agg = movies.groupBy("genres").count()

    agg.write.mode(SaveMode.Overwrite).saveAsTable("test.t_agg_genres")

  }



  test("write to hive with partition should create partition table first") {
    val schemaString = "name,age,month"
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val rdd = sparkContext.parallelize[(String, String, String)](Seq(("a1", "1", "2016-05"), ("a2", "2", "2016-06")))

    val rowRDD = rdd.map(line => Row(line._1, line._2, line._3))


    val df = sqlContext.createDataFrame(rowRDD, schema)

    df.show()



      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    df.write.mode(SaveMode.Append).partitionBy("month").saveAsTable("test.t_test")

  }


  test("write movies to mongo rdd") {
    val moviesRDD = sparkContext.textFile("/Users/yxl/data/ml-1m/movies.dat")
      .filter(line => {
        StringUtils.isNotEmpty(line)
      })
      .map(line => {
        val array = line.split("::")
        val movie = Movie(array(0).toInt, array(1), array(2))
        Document.parse(JsonUtil.toJsonString(movie))
      })

    MongoSpark.save(moviesRDD, WriteConfig(Map("uri" -> "mongodb://127.0.0.1/recommend.movies")))
  }

  override protected def afterAll(): Unit = {
    sparkContext.stop()
  }

}


object JsonUtil {

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  def toJsonString(o: Any): String = {
    objectMapper.writeValueAsString(o)
  }

}
