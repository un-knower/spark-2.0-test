package ml.decision_tree.classification

import ml.BaseML
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._

/**
  * Created by yxl on 2017/12/16.
  */
object DecisionTreeClassifierML extends BaseML{

    override val appName = "DecisionTreeClassifierML"

    def main(args: Array[String]): Unit = {

        // 处理特征 -> 向量
        val colArray = Array("sepal_length","sepal_width","petal_length","petal_width")
        val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")

        val vectorDataFrame  = assembler.transform(irisDataFrame)

        // 处理样本特征和标签
        val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(vectorDataFrame)

        val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(vectorDataFrame)

        val indexString = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictionLabel")
        .setLabels(labelIndexer.labels)

        val Array(trainningData,testData) = vectorDataFrame.randomSplit(Array(0.7,0.3))

        // 分类决策树
        val decisionTree = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")

        // 构建pipeline
        val pipline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, decisionTree, indexString))

        val model = pipline.fit(trainningData)

        val predictions = model.transform(testData)

        predictions.show()

        val accuracy = new MulticlassMetrics(predictions.select(col("indexedLabel"),col("prediction")).rdd.map(
            row => (row.getDouble(0), row.getDouble(1)))).accuracy
        println(s"  Accuracy : $accuracy")


        val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]

        val paramMap = treeModel.extractParamMap()

        println(s"paramMap:${paramMap}")

        println("Learned ml.regression tree model:\n" + treeModel.toDebugString)

    }

}
