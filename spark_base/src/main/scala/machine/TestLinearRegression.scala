package machine

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater, SimpleUpdater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by yxl on 15/8/2.
 */
object TestLinearRegression {

    def main(args:Array[String]): Unit ={

      val sparkConf = new SparkConf().setAppName("TestLinearRegression")
        .setMaster("local[4]")
        .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
        .set("spark.driver.memory", "128m")
        .set("spark.local.dir", "/Users/yxl/data/spark.dir")

      val sc = new SparkContext(sparkConf);

      //val input = "/Users/yxl/data/mllib/sample_linear_regression_data.txt"
      val input = "/Users/yxl/data/mllib/simple_data"

      val examples = MLUtils.loadLibSVMFile(sc,input).cache()

      val scaler = new StandardScaler(withMean = true, withStd = true)
        .fit(examples.map(x => x.features))

      val scaledData = examples
        .map(x =>
        LabeledPoint(x.label,
          scaler.transform(Vectors.dense(x.features.toArray))))


      val splits = scaledData.randomSplit(Array(0.8, 0.1))

      val training = splits(0).cache()
      val test = splits(1).cache()

      val numTraining = training.count()
      val numTest = test.count()
      println(s"Training: $numTraining, test: $numTest.")

      examples.unpersist(blocking = false)


      //test.collect()

      test.foreach(println(_))


      val updaterMap = Map("L0" -> new SimpleUpdater() ,
                        "L1" -> new L1Updater() ,
                        "L2" -> new SquaredL2Updater())

      val numIterations = 1000
      val stepSize = 0.2
      val update = "L0"
      val regParam = 0.01

      val algorithm = new LinearRegressionWithSGD()
      algorithm.optimizer  // 针对梯度下降的参数设置
        .setNumIterations(numIterations)
        .setStepSize(stepSize)
        .setUpdater(updaterMap.get(update).get)
        .setRegParam(regParam)

      algorithm.setIntercept(true) // 截距

      val model = algorithm.run(training)

      println("weights :" + model.weights)

      println("intercept :" + model.intercept)

      val prediction = model.predict(test.map(_.features))

      val predictionAndLabel = prediction.zip(test.map(_.label))

      val loss = predictionAndLabel.map { case (p, l) =>
        println(p + " " + l)
        val err = p - l
        err * err
      }.reduce(_ + _)

      val rmse = math.sqrt(loss / numTest)

      println(s"Test RMSE = $rmse.")

      sc.stop()


    }

}
