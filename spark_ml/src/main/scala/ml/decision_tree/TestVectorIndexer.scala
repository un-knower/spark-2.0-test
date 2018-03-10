package ml.decision_tree

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by yxl on 2017/12/4.
  */
object TestVectorIndexer {

    /**
      *
      *  2.0,5.0,7.0
      *  3.0,5.0,9.0
      *  4.0,7.0,9.0
      *  2.0,4.0,9.0
      *  9.0,5.0,7.0
      *  2.0,5.0,9.0
      *
      * @param args
      */

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
        .builder
        .master("local[2]")
        .appName("LogisticRegressionML")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()


        val vectors = Seq(
            Vectors.sparse(3,Array(0,1,2),Array(2.0,5.0,7.0)),
            Vectors.sparse(3,Array(0,1,2),Array(3.0,5.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(4.0,7.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(2.0,4.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(9.0,5.0,7.0)),
            Vectors.sparse(3,Array(0,1,2),Array(2.0,5.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(3.0,4.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(8.0,4.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(3.0,6.0,2.0)),
            Vectors.sparse(3,Array(0,1,2),Array(5.0,9.0,2.0))
        )

        import spark.implicits._

        val data = spark.createDataFrame(vectors.map(Tuple1.apply)).toDF("features")

        data.show()

        val featureIndexerModel=new VectorIndexer()
        .setInputCol("features")
        .setMaxCategories(5)
        .setOutputCol("indexedFeatures")
        .fit(data)

        val  featureData =  featureIndexerModel.transform(data)

        featureData.foreach(println(_))
    }

}
