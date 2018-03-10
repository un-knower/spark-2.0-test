package machine

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater, SimpleUpdater}
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yxl on 15/10/10.
 */

object TestPredictOrder  {

   def main(args:Array[String]): Unit ={
     val sparkConf = new SparkConf().setAppName("TestPredictOrder")
       .setMaster("local[4]")
       .set("spark.cores.max", "3").set("spark.executor.memory", "1024m")
       .set("spark.driver.memory", "512m")
       .set("spark.local.dir", "/Users/yxl/data/spark.dir")

     val sc = new SparkContext(sparkConf);

     val input = "/Users/yxl/data/mllib/t_orders_all.csv"

     val orderRDD = sc.textFile(input)

     def checkDate(dstring:String):Boolean = {
       val format = new SimpleDateFormat("yyyy-MM-dd HH:mm")
       val tmpDate = format.parse(dstring)
       val calendar = Calendar.getInstance()
       calendar.setTime(tmpDate)
       val hour = calendar.get(Calendar.HOUR_OF_DAY)
       hour >= 19
     }

     val filterRDD = orderRDD.map(x => x.split(","))
       .map(x => (x(1).replaceAll("\"",""),x(2).toInt))
       .filter(x => checkDate(x._1))

     filterRDD.cache()

     val baseRDD = filterRDD.map( x=> (x._1, x._2))

     def addMinute(dstring:String,minute:Int):String = {
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm")
          val tmpDate = format.parse(dstring)
          val calendar = Calendar.getInstance()
          calendar.setTime(tmpDate)
          calendar.add(Calendar.MINUTE, minute)
          format.format(calendar.getTime())
     }

     val lastOneMinuteRDD = filterRDD.map( x=> (addMinute(x._1,10) , x._2))

     val lastTwoMinuteRDD = filterRDD.map(x=>(addMinute(x._1,20), x._2))

     val lastThreeMinuteRDD = filterRDD.map(x=>(addMinute(x._1,30) , x._2))

     val lastFourMintueRDD = filterRDD.map(x=>(addMinute(x._1,40) , x._2))

     def checkValue(value:Option[Int]):Int = {
       value match {
         case Some(value) => value
         case None => 0
       }
     }

     def mergeValue(value:Int,append:Option[Int]):Array[Int] = {
       Array(value, checkValue(append))
     }

     def mergeArrayValue(value:Array[Int],append:Option[Int]):Array[Int] = {
        val buffer = new ArrayBuffer[Int]()
        buffer.++=(value).append(checkValue(append))
        buffer.toArray
     }

     val joinRDD = baseRDD.leftOuterJoin(lastOneMinuteRDD).map( x=> (x._1,mergeValue(x._2._1,x._2._2)))
         .leftOuterJoin(lastTwoMinuteRDD).map(x =>(x._1,mergeArrayValue(x._2._1,x._2._2)))
         .leftOuterJoin(lastThreeMinuteRDD).map(x => (x._1,mergeArrayValue(x._2._1,x._2._2)))
         .leftOuterJoin(lastFourMintueRDD).map(x => (x._1,mergeArrayValue(x._2._1,x._2._2)))
         .map( x=> {
           val value = x._2
           val key = value.apply(0)
           val indices = new ArrayBuffer[Int]()
           val values = new ArrayBuffer[Double]()
           value.zipWithIndex.map(x => { if(x._2> 0) {indices.append(x._2-1) ; values.append(x._1)} })
            LabeledPoint(key, Vectors.sparse(value.size-1,indices.toArray,values.toArray))
         })


     val scaler = new StandardScaler(withMean = true, withStd = true).fit(joinRDD.map(x => x.features))

     val scaledData = joinRDD
       .map(x =>
       LabeledPoint(x.label,scaler.transform(Vectors.dense(x.features.toArray))))


     val splitRDD = scaledData.randomSplit(Array(0.8,0.2))
     val trainRDD = splitRDD(0).cache()
     val testRDD = splitRDD(1).cache()

     //trainRDD.take(100).foreach(println(_))

    println("trainRDD:" + trainRDD.count() + "  testRDD:" + testRDD.count())

     filterRDD.unpersist()


     val updaterMap = Map("L0" -> new SimpleUpdater() ,
       "L1" -> new L1Updater() ,
       "L2" -> new SquaredL2Updater())

     val numIterations = 1000
     val stepSize = 1
     val update = "L1"
     val regParam = 0.01

     val algorithm = new LinearRegressionWithSGD()
     algorithm.optimizer  // 针对梯度下降的参数设置
       .setNumIterations(numIterations)
       .setStepSize(stepSize)
       .setUpdater(updaterMap.get(update).get)
       .setRegParam(regParam)

     algorithm.setIntercept(true)

     val model = algorithm.run(trainRDD)

     println("weights :" + model.weights)

     println("intercept :" + model.intercept)


     val prediction = model.predict(testRDD.map(_.features))

     val predictionAndLabel = prediction.zip(testRDD.map(_.label))

     val loss = predictionAndLabel.map { case (p, l) =>
       println(p.toInt + "\t" + l)
       val err = p - l
       err * err
     }.reduce(_ + _)

     val rmse = math.sqrt(loss / testRDD.count())

     println(s"Test RMSE = $rmse.")

     sc.stop()

   }
}
