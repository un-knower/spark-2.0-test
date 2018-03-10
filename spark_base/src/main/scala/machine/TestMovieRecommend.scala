package machine

import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jblas.DoubleMatrix

/**
 * Created by yxl on 16/1/13.
 */
object TestMovieRecommend {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestMovieRecommend")
      .setMaster("local[4]")
      .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
      .set("spark.driver.memory", "512m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf)

    LogManager.getRootLogger.setLevel(Level.ERROR)

    // val base = "/ml-1m/"

    val base = "/Users/yxl/data/ml-1m/"

    // UserID::MovieID::api.Rating::Timestamp
    val ratings = sc.textFile(base + "ratings.dat").map { line =>
      val fields = line.split("::")
      // format: (timestamp % 10, api.Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    // MovieID::Title::Genres
    val movies = sc.textFile(base + "movies.dat").map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }.collectAsMap()


    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")


    val numPartitions = 4

    val training = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).cache()

    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).cache()

    val test = ratings.filter(x => x._1 >= 8).values.cache()


    /**
     * 在 1.4 的版本上有bug 迭代次数比较小 否则 StackOverflowError
     * 增加 checkPointInterval 需要设置 checkpoint
     */
    sc.setCheckpointDir("/Users/yxl/data/checkpoint")
    //training.checkpoint()
    //validation.checkpoint()
    //test.checkpoint()


    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    val ranks = List(8,12,20) // feature 的 数量,默认10
    val lambdas = List(0.01,0.1)
    val numIters = List(3,5,10,20)
    val alpha = 0.01
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = new ALS()
        .setRank(rank)
        .setIterations(numIter)
        .setLambda(lambda)
        .setImplicitPrefs(false)
        .setCheckpointInterval(5) // 默认 10 有点大
        .run(training)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }


    /** Compute RMSE (Root Mean Squared Error). */
    def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
      val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
      val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
        .join(data.map(x => ((x.user, x.product), x.rating)))
        .values
      math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
    }


    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    val singleTest = test.take(1).apply(0)

    val userId = singleTest.user

    val movieId = singleTest.product

    val predictRating = bestModel.get.predict(userId,movieId)  // 获取商品评分

    println(s"api.Rating user $userId product $movieId  rate:$predictRating")

    val recommendations = bestModel.get.recommendProducts(userId,5) // 获取推荐商品

    recommendations.foreach(println(_)) // 可能的评分

    // 为每个用户推荐
    val recommendForUsers = bestModel.get.recommendProductsForUsers(5)
    println(recommendForUsers.count())



    //相似的电影
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double =
    {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }

    val itemId = 567
    val itemFactor = bestModel.get.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    cosineSimilarity(itemVector, itemVector) // 相似度为 1

    val sims = bestModel.get.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }

    val sortedSims = sims.filter({case(id,similarity) => similarity < 1}).top(5)(Ordering.by[(Int, Double), Double]({ case
      (id, similarity) => similarity }))

    sortedSims.foreach(x => println(movies(x._1))) // 电影名称


    import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

    //全部相似电影 top(5)
    val simsForMovies = bestModel.get.productFeatures
          .cartesian(bestModel.get.productFeatures)
          .map({case((id,vec1),(sid,vec2)) => {
              (id,(sid,cosineSimilarity(new DoubleMatrix(vec1),new DoubleMatrix(vec2))))
            }
          }).filter({
               case(id,(sid,sim)) =>{
                  id != sid
               }
          }).topByKey(5)(Ordering.by({
            case (sid,sim) => sim
          }))

    simsForMovies.take(10).foreach(line => println(line._1 + " " + line._2.mkString(",")))

    // compute MSE
    val usersProducts = ratings.map(item => {
          item._2 match {
            case Rating(user, product, rating)  => (user, product)}
          })

    val predictions = bestModel.get.predict(usersProducts).map({
      case Rating(user, product, rating) => ((user, product), rating)
    })

    val ratingsAndPredictions = ratings.map(item => {
      item._2 match {
        case Rating(user, product, rating) => ((user, product), rating)
      }
    }).join(predictions)

    val MSE = ratingsAndPredictions.map({
      case ((user, product), (actual, predicted)) =>  math.pow((actual -
        predicted), 2)
    }).reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)



    val meanRating = training.union(validation).map(_.rating).mean

    val baselineRmse =
      math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)

    val improvement = (baselineRmse - testRmse) / baselineRmse * 100

    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")


  }

}
