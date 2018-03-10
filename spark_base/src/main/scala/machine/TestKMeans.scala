package machine

import org.apache.commons.lang3.StringUtils
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/8/9.
 */
object TestKMeans {
   def main(args:Array[String]): Unit ={

     val sparkConf = new SparkConf().setAppName("TestLinearRegression")
       .setMaster("local[2]")
       .set("spark.cores.max", "3").set("spark.executor.memory", "128m")
       .set("spark.driver.memory", "128m")
       .set("spark.local.dir", "/Users/yxl/data/spark.dir")

     val sc = new SparkContext(sparkConf);


     val data = sc.textFile("/Users/yxl/data/mllib/kmeans_data.txt")

     val filterData = data.filter( x => {
        if(StringUtils.isEmpty(x)){
           false
        }else{
          true
        }
     })

     filterData.cache()

     val labeledPointData = filterData.map(x =>{
          val array = x.split(" ")
          val label = array.apply(0)
          val point = Vectors.dense(array.apply(1).split(",").map(_.toDouble))
          (point,label)
     })

     val parsedData = labeledPointData.map(x => x._1).cache()

     val count = parsedData.count()
     println(s" total number : $count")

     // Cluster the data into two classes using KMeans
     val numClusters = 2
     val numIterations = 20
     val clusters = KMeans.train(parsedData,numClusters, numIterations)



     val analyData = parsedData.map(x => {
       val cluster = clusters.predict(x)
       (x,cluster)
     })

     analyData.join(labeledPointData).collect().foreach( x => println(x._2))

     // Evaluate clustering by computing Within Set Sum of Squared Errors
     val WSSSE = clusters.computeCost(parsedData)
     println("Within Set Sum of Squared Errors = " + WSSSE)



   }
}
