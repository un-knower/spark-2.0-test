package ml

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by yxl on 2017/12/18.
  */
trait BaseML {

    protected  val appName : String

    lazy val spark = SparkSession
    .builder
    .master("local[2]")
    .appName(appName)
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

    private val basePath = "/Users/yxl/data/mllib"

    lazy val advertisingDataFrame = spark.read.format("csv")
    .option("header", true)
    .schema(StructType(Seq(
        StructField("id", DataTypes.IntegerType),
        StructField("TV", DataTypes.DoubleType),
        StructField("Radio", DataTypes.DoubleType),
        StructField("Newspaper", DataTypes.DoubleType),
        StructField("label", DataTypes.DoubleType)
    )))
    .load(basePath + "/Advertising.csv")

    lazy val irisDataFrame = spark.read.format("csv")
    .option("header", true)
    .schema(StructType(Seq(
        StructField("sepal_length", DataTypes.DoubleType),
        StructField("sepal_width", DataTypes.DoubleType),
        StructField("petal_length", DataTypes.DoubleType),
        StructField("petal_width", DataTypes.DoubleType),
        StructField("label", DataTypes.StringType)
    )))
    .load(basePath + "/iris.data")


    def parseAdvertisingDataFrame() ={
        val colArray = Array("TV","Radio","Newspaper")
        val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
        val vectorDataFrame = assembler.transform(advertisingDataFrame)
        vectorDataFrame
    }

    def parseIrisDataFrame() = {
        val colArray = Array("sepal_length","sepal_width","petal_length","petal_width")
        val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
        val vectorDataFrame  = assembler.transform(irisDataFrame)
        vectorDataFrame
    }

}
