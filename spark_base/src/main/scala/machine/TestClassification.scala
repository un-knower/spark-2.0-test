package machine

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, NaiveBayes, SVMWithSGD, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 16/1/14.
 *
 * 测试各种分类算法
 */
object TestClassification {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestMovieReend")
      .setMaster("local[4]")
      .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
      .set("spark.driver.memory", "512m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf)

    LogManager.getRootLogger.setLevel(Level.ERROR)

    val base = "/Users/yxl/data/train_noheader.tsv"

    val rawData = sc.textFile(base)

    val records = rawData.map(line => line.split("\t"))

    val nbData =  records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d ==
        "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)
      LabeledPoint(label, Vectors.dense(features))
    }

    val cacheNBData = nbData.cache()

    val numData = cacheNBData.count()

    println("总数:" + numData)


    val vectors = cacheNBData.map(lp => lp.features)
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics()
    // println(matrixSummary.mean)

    // 标准化
    val scaler = new StandardScaler(withMean = true, withStd =
      true).fit(vectors)
    val scaledData = cacheNBData.map(lp => LabeledPoint(lp.label,scaler.transform(lp.features)))


    val numIterations = 50
    val maxTreeDepth = 5

    // 逻辑回归
    val lrModel = LogisticRegressionWithSGD.train(cacheNBData, numIterations)
    val lrTotalCorrect = cacheNBData.map { point =>
      if (lrModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val lrAccuracy = lrTotalCorrect / cacheNBData.count
    println("逻辑回归正确率:" + lrAccuracy)

    // 标准化后逻辑回归
    val lrModelScaled = LogisticRegressionWithSGD.train(scaledData,numIterations)
    val lrTotalCorrectScaled = scaledData.map { point =>
      if (lrModelScaled.predict(point.features) == point.label) 1 else 0 }.sum
    val lrAccuracyScaled = lrTotalCorrectScaled / numData
    println("标准化后逻辑回归正确率:"+ lrAccuracyScaled)


    // 支持向量机
    val svmModel = SVMWithSGD.train(cacheNBData, numIterations)
    val svmTotalCorrect = cacheNBData.map { point =>
      if (svmModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val svmAccuracy =  svmTotalCorrect / cacheNBData.count
    println("SVM正确率:" + svmAccuracy)

    // 贝叶斯
    val nbModel = NaiveBayes.train(cacheNBData)
    val nbTotalCorrect = nbData.map { point =>
      if (nbModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val nbAccuracy = nbTotalCorrect / cacheNBData.count
    println("贝叶斯正确率:" + nbAccuracy )


    // 决策树
    val dtModel = DecisionTree.train(cacheNBData, Algo.Classification, Entropy,
      maxTreeDepth)
    val dtTotalCorrect = cacheNBData.map { point =>
      val score = dtModel.predict(point.features)
      val predicted = if (score > 0.5) 1 else 0
      if (predicted == point.label) 1 else 0
    }.sum
    val dtAccuracy = dtTotalCorrect / cacheNBData.count
    println("决策树正确率:" + dtAccuracy)


    val dataPoint = cacheNBData.take(10).apply(3)
    val prediction = lrModel.predict(dataPoint.features)
    val trueLabel = dataPoint.label
    println(dataPoint.features.apply(1) + " 分类值:" + prediction + " 实际值:" + trueLabel)

    //执行多个分类
    val predictions = lrModel.predict(cacheNBData.map(lp => lp.features))
    predictions.take(5)



    // ROC
    // TPR=TP / (TP + FN)
    val metrics = Seq(lrModel, svmModel).map { model =>
      val scoreAndLabels = cacheNBData.map { point =>
        (model.predict(point.features), point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }

    val nbMetrics = Seq(nbModel).map{ model =>
      val scoreAndLabels = nbData.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR,metrics.areaUnderROC)
    }


    val dtMetrics = Seq(dtModel).map{ model =>
      val scoreAndLabels = cacheNBData.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR,metrics.areaUnderROC)
    }

    val allMetrics = metrics ++ nbMetrics ++ dtMetrics
    allMetrics.foreach{ case (m, pr, roc) =>
      println(s"$m, Area under PR: ${pr * 100.0} , Area under ROC: ${roc * 100.0}")
    }

  }

}
