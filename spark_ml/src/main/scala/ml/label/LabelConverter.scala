package ml.label

import ml.BaseML
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

/**
  * Created by yxl on 2017/12/16.
  */
object LabelConverter extends BaseML{

    override val appName = "LabelConverter"

    def main(args: Array[String]): Unit = {

        val df = spark.createDataFrame(Seq((0,"a"),(1,"b"),(2,"c"),(3,"b"),(4,"a"))).toDF("id", "label")

        val stringIndexer = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("indexLabel")
            .fit(df)

        val indexed = stringIndexer.transform(df)

        println(stringIndexer.labels.mkString(","))

        indexed.show()

        val indexString = new IndexToString()
            .setInputCol("indexLabel")
            .setOutputCol("labelString")
            .setLabels(stringIndexer.labels)
        val stringed = indexString.transform(indexed)
        stringed.show()


    }

}
