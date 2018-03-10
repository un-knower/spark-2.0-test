package ml.decision_tree.classification

import ml.BaseML
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

/**
  * Created by yxl on 2017/12/21.
  */
object RandomForestClassifierML extends BaseML {

    override protected val appName: String = "RandomForestClassifier"

    def main(args: Array[String]): Unit = {

        val  irisData = super.parseIrisDataFrame()

        println(s"count:${irisData.count}")

        val Array(trainingData,testData) = irisData.randomSplit(Array(0.7,0.3))

        // 处理样本特征和标签
        val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(irisData)


        val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(irisData)

        val rf = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setFeatureSubsetStrategy("sqrt")
        .setImpurity("entropy")
        .setMinInfoGain(0.001)
        .setSubsamplingRate(0.8)
        .setMinInstancesPerNode(1)
        .setMaxDepth(5)

        val labelConverter = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictionLabel")
            .setLabels(labelIndexer.labels)

        val pipeline = new Pipeline()
            .setStages(Array(labelIndexer,featureIndexer,rf,labelConverter))


        val model = pipeline.fit(trainingData)

        val predictions = model.transform(testData)

        predictions.show()

        val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"accuracy:${accuracy}")


        val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
        val numTrees = rfModel.trees.size
        println(s"numTrees:${numTrees}")

        println("Learned classification forest model:\n" + rfModel.toDebugString)

    }


}
