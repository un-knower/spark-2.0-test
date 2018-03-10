package ml.decision_tree.regression

import ml.BaseML
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

/**
  * Created by yxl on 2017/12/18.
  */

object RandomForestRegressorML extends BaseML {

   override val appName = "RandomForestRegressorML"

    def main(args: Array[String]): Unit = {

        val df = super.parseAdvertisingDataFrame()

        val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(df)

        val Array(trainningData, testData) = df.randomSplit(Array(0.7,0.3))

        val rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("indexedFeatures")
                .setFeatureSubsetStrategy("sqrt")

        val pipeline = new Pipeline()
            .setStages(Array(featureIndexer, rf))

        val model = pipeline.fit(trainningData)

        val predictions = model.transform(testData)

        predictions.show()

        val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")

        val rmse = evaluator.evaluate(predictions)

        println(s"rmse:${rmse}")

        val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
        println(rfModel.toDebugString)

        rfModel.transform(testData)

    }
}
