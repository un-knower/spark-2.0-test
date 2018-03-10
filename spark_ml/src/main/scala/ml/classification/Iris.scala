package ml.classification

import java.util.concurrent.TimeUnit

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable

/**
  * 逻辑回归 鸢尾花 数据
  */
object Iris {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
        .builder
        .master("local[2]")
        .appName("LogisticRegressionML")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()

        val basePath = "/Users/yxl/data/mllib"


        val data = spark.read.format("csv")
                .option("header", true)
                .schema(StructType(Seq(
                    StructField("sepal_length", DataTypes.DoubleType),
                    StructField("sepal_width", DataTypes.DoubleType),
                    StructField("petal_length", DataTypes.DoubleType),
                    StructField("petal_width", DataTypes.DoubleType),
                    StructField("label", DataTypes.StringType)
                )))
                .load(basePath + "/iris.data")

        data.show()

        require(data.count() > 0, s"数据需要>0")

        val dataframes: Array[DataFrame] = data.randomSplit(Array(0.8,0.2), seed = 123456)

        val train = dataframes(0).cache()
        val test = dataframes(1).cache()

        val trainCount = train.count()
        val testCount = test.count()

        println(s"count train:$trainCount test:$testCount")


        val stages = new mutable.ArrayBuffer[PipelineStage]()

        val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")

        stages += labelIndexer

        val colArray = Array("sepal_length","sepal_width","petal_length","petal_width")
        val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")

        stages += assembler

        val lor = new LogisticRegression()
        .setFeaturesCol("features")
        .setLabelCol("indexedLabel")
        .setRegParam(0.01)
        .setElasticNetParam(0.8)
        .setMaxIter(100)
        .setFitIntercept(true)
        .setFamily("multinomial")

        stages += lor

        val pipeline = new Pipeline().setStages(stages.toArray)

        val pipelineModel = pipeline.fit(train)

        val testPredictions = pipelineModel.transform(test).cache()

        // probability 表示数据哪个类别的概率
        testPredictions.select("probability","prediction").collect().foreach(
            row => println(row.get(0), row.get(1))
        )

        val predictions = testPredictions.select("prediction").rdd.map(_.getDouble(0))
        val labels = testPredictions.select("indexedLabel").rdd.map(_.getDouble(0))

        val accuracy = new MulticlassMetrics(predictions.zip(labels)).accuracy
        println(s"  Accuracy : $accuracy")

        println(s"pipelineModel:${pipelineModel.stages.mkString(",")}")



        // TimeUnit.MINUTES.sleep(10)
        spark.stop()

    }

}
