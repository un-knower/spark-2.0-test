package machine

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation._

/**
 * Created by yxl on 16/1/11.
 */
object TestMusicRecommend  {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("TestRecommend")
      .setMaster("local[4]")
      .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
      .set("spark.driver.memory", "512m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf)


    // 数据集下载位置 http://www-etud.iro.umontreal.ca/~bergstrj/audioscrobbler_data.html
    val base = "/Users/yxl/data/profiledata_06-May-2005/"

    val rawUserArtistData = sc.textFile(base + "user_artist_data.txt")


    rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
    rawUserArtistData.map(_.split(' ')(1).toDouble).stats()

    val rawArtistData = sc.textFile(base + "artist_data.txt")
    val artistByID = rawArtistData.flatMap(line => {
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    })

    val rawArtistAlias = sc.textFile(base + "artist_alias.txt")
    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()


    val bArtistAlias = sc.broadcast(artistAlias)

    val trainData = rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }.cache()

    /**
     * 参数设置
     * rank = 10
     * iterations = 5
     * lambda = 0.01
     * alpha = 1
     */
    val model = ALS.trainImplicit(trainData,5,2, 0.01, 1.0)

    // [(key,value)]
    println("user features")
    model.userFeatures.mapValues(_.mkString(", ")).take(10).foreach(println(_))

    println("product features")
    model.productFeatures.mapValues(_.mkString(", ")).take(10).foreach(println(_))


    val rawArtistsForUser = rawUserArtistData.map(_.split(' '))
      .filter { case Array(user,_,_) => user.toInt == 2093760 }

    val existingProducts = rawArtistsForUser.map {
      case Array(_,artist,_) => artist.toInt
    }.collect().toSet


    artistByID.filter { case (id, name) =>
      existingProducts.contains(id)
    }.values.collect().foreach(println)


    val recommendations = model.recommendProducts(2093760, 5)

    recommendations.foreach(println)

    val recommendedProductIDs = recommendations.map(_.product).toSet

    artistByID.filter { case (id, name) => recommendedProductIDs.contains(id)
    }.values.collect().foreach(println)


  }

}
