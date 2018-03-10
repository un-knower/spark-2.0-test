package machine

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils


/**
 * Created by yxl on 15/8/7.
 */
object TestLogisticRegression {

  def main(args:Array[String]){

    val sparkConf = new SparkConf().setAppName("TestLogisticRegression")
      .setMaster("local[4]")
      .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
      .set("spark.driver.memory", "128m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf);

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "/Users/yxl/data/mllib/sample_libsvm_data.txt")



    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      println("prediction:" + prediction + "    " + "label:" + label)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)

    // Save and load model
    //model.save(sc, "myModelPath")
    //val sameModel = LogisticRegressionModel.load(sc, "myModelPath")

  }

}
