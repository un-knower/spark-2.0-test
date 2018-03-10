package ml.regression

import ml.BaseML
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics

/**
  * 线性回归 Advertising 广告数据
  */
object Advertising extends BaseML{

    override val appName = "Advertising"

    def main(args: Array[String]): Unit = {

        val colArray = Array("TV","Radio","Newspaper")
        val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
        val vectorDataFrame = assembler.transform(advertisingDataFrame)

        val lr = new LinearRegression()
        .setMaxIter(100)
        .setRegParam(0.003)
        .setLabelCol("Sales")
        .setFeaturesCol("features")
        .setElasticNetParam(0.8)

        val lrModel = lr.fit(vectorDataFrame)

        println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

        val trainingSummary = lrModel.summary
        println(s"numIterations: ${trainingSummary.totalIterations}")
        println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
        println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
        println(s"r2: ${trainingSummary.r2}")


        val predictions = lrModel.transform(vectorDataFrame).select("prediction").rdd.map(_.getDouble(0))
        val labels = vectorDataFrame.select("Sales").rdd.map(_.getDouble(0))

        val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError

        println(s"RMSE:${RMSE}")

    }

}
