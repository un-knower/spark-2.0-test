package ml.decision_tree.regression

import ml.BaseML
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}

/**
  * Created by yxl on 2017/12/19.
  */
object GBTRegressorML extends BaseML{

    override val appName = "GBTRegressorML"

    def main(args: Array[String]): Unit = {

        val data = super.parseAdvertisingDataFrame()

        val featureIndex = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4)
            .fit(data)

        val Array(trainingData,testData) = data.randomSplit(Array(0.7,0.3))

        val gbt = new GBTRegressor()
            .setLabelCol("label")
            .setFeaturesCol("indexedFeatures")
            .setMaxIter(20)
            .setLossType("absolute")

        val pipeline = new Pipeline()
            .setStages(Array(featureIndex, gbt))

        val model = pipeline.fit(trainingData)

        val predictions = model.transform(testData)

        predictions.show()

        val evaluator = new RegressionEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("rmse")

        val rmse = evaluator.evaluate(predictions)

        println(s"rmse:${rmse}")


        val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]


        println(s"train trees: ${gbtModel.numTrees}")

        //println(gbtModel.toDebugString)

    }
}
