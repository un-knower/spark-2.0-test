package ml.classification

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionTrainingSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.mutable

/**
  * 逻辑回归
  */
object LogisticRegressionML {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
        .builder
        .master("local[2]")
        .appName("LogisticRegressionML")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()

        val basePath = "/Users/yxl/data/mllib"

        val data = spark.read.format("libsvm").load(s"${basePath}/sample_libsvm_data.txt")

        val dataframes: Array[DataFrame] = data.randomSplit(Array(0.8,0.2), seed = 123456)

        val train = dataframes(0).cache()
        val test = dataframes(1).cache()

        val trainCount = train.count()
        val testCount = test.count()

        println(s"count train:$trainCount test:$testCount")

        train.printSchema()

        val labelIndexerModel = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(data)

        val stages = new mutable.ArrayBuffer[PipelineStage]()

        stages += labelIndexerModel

        val lor = new LogisticRegression()
        .setFeaturesCol("features")
        .setLabelCol("indexedLabel")
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
        .setMaxIter(100)
        .setFitIntercept(true)
        .setFamily("binomial")

        stages += lor

        // 通过 pipeline 的方式 参数为多个stage
        val pipeline = new Pipeline().setStages(stages.toArray)

        // 训练模型
        val pipelineModel = pipeline.fit(train)

        // 逻辑回归模型
        val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]

        // 逻辑回归模型参数
        val params = lorModel.extractParamMap()
        println(s"params:${params}")

        // 模型参数
        println(s"coefficients: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")

        val trainingSummary = lorModel.summary.asInstanceOf[BinaryLogisticRegressionTrainingSummary]

        // 每次迭代的损失函数
        val objectiveHistory = trainingSummary.objectiveHistory
        println("objectiveHistory:")
        objectiveHistory.foreach(loss => println(loss))

        // 计算ROC
        val roc = trainingSummary.roc
        roc.show()

        // aoc
        val aoc = trainingSummary.areaUnderROC
        println(s"aoc:${aoc}")


        // 预测
        val testPredictions = pipelineModel.transform(test).cache()
        testPredictions.show()

        val predictions = testPredictions.select("prediction").rdd.map(_.getDouble(0))
        val labels = testPredictions.select("indexedLabel").rdd.map(_.getDouble(0))

        val metrics = new MulticlassMetrics(predictions.zip(labels))

        // 准确率
        val accuracy = metrics.accuracy
        println(s"Accuracy : $accuracy")


        //不同的阈值，计算不同的F1，然后通过最大的F1找出并重设模型的最佳阈值
        val fMeasure = trainingSummary.fMeasureByThreshold
        val maxFMeasure = fMeasure.select(functions.max("F-Measure")).head.getDouble(0)
        //获得最大的F1值
        val bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure)).select("threshold").head.getDouble(0)//找出最大F1值对应的阈值（最佳阈值）
        lorModel.setThreshold(bestThreshold)//并将模型的Threshold设置为选择出来的最佳分类阈值
        println(s"Threshold:${bestThreshold}")

        spark.stop()

    }
}
