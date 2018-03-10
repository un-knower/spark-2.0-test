package ml.decision_tree.regression

import ml.BaseML
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}

/**
  * Created by yxl on 2017/12/16.
  */
object DecisionTreeRegressorML extends BaseML {

    override val appName = "DecisionTreeRegressorML"

    def main(args: Array[String]): Unit = {

        // 处理特征 -> 向量
        val colArray = Array("TV","Radio","Newspaper")
        val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")

        val vectorDataFrame = assembler.transform(advertisingDataFrame)


        val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(vectorDataFrame)


        val Array(trainningData,testData) = vectorDataFrame.randomSplit(Array(0.7,0.3))

        val decisionTree = new DecisionTreeRegressor()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")

        // 构建pipeline
        val pipline = new Pipeline()
        .setStages(Array(featureIndexer, decisionTree))

        val model = pipline.fit(trainningData)

        val predictions = model.transform(testData)

        predictions.show()

        val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
        val rmse = evaluator.evaluate(predictions)
        println("Root Mean Squared Error (RMSE) on test data = " + rmse)

        val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]

        val paramMap = treeModel.extractParamMap()
        println(s"paramMap:${paramMap}")

        println("Learned ml.regression tree model:\n" + treeModel.toDebugString)

    }
}
