package machine

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater, SimpleUpdater}
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.mllib.tree.{impurity, DecisionTree}
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.{Variance, Entropy}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 16/1/15.
 */
object TestRegression {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestMovieRecommend")
      .setMaster("local[4]")
      .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
      .set("spark.driver.memory", "512m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf)


    val base = "/bike/"

    val data = sc.textFile(base + "day.csv")

    val records = data.map( line => {
      val split = line.split(",")
      val label = split(split.size - 1).toInt
      val features = split.slice(2, split.size - 1).map(d =>
        if (d =="?") 0.0 else d.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    })


    // 归一化
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(records.map(x => x.features))
    val scaledData = records.map(x =>LabeledPoint(x.label,scaler.transform(Vectors.dense(x.features.toArray))))

    val cachedData = scaledData.cache()

    val numIterations = 100

    val updaterMap = Map("L0" -> new SimpleUpdater() ,"L1" -> new L1Updater() ,"L2" -> new SquaredL2Updater())

    val stepSize = 0.2
    val update = "L2"
    val regParam = 0.01

    val linearRegression = new LinearRegressionWithSGD()

    // 针对梯度下降的参数设置
    linearRegression.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setUpdater(updaterMap.get(update).get).setRegParam(regParam) // 防止过拟合

    // 截距
    linearRegression.setIntercept(true)

    // 线性回归
    val lrModel = linearRegression.run(cachedData)


    // 计算RMSE
    val testRDD = cachedData.sample(false,0.5)

    val testRDDCount = testRDD.count()

    val prediction = lrModel.predict(testRDD.map(_.features))

    val predictionAndLabel = prediction.zip(testRDD.map(_.label))

    val loss = predictionAndLabel.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)

    val rmse = math.sqrt(loss / testRDDCount )



    // 决策树
    val maxTreeDepth = 5
    val dtModel = DecisionTree.train(cachedData, Algo.Regression,Variance,maxTreeDepth)

    val dtPrediction = dtModel.predict(testRDD.map(_.features))

    val dtPredictionAndLabel = dtPrediction.zip(testRDD.map(_.label))

    val dtLoss = dtPredictionAndLabel.map{
      case (p,l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)

    val dtRmse = math.sqrt(dtLoss / testRDDCount)


    //

  }

}
