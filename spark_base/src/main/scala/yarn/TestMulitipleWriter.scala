package yarn

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/12/28.
 */
class TestMulitipleWriter {

  val sparkConf = new SparkConf().setAppName("TestMultipleOutput")
  //.setMaster("local[4]")
  //.set("spark.cores.max", "1")
  //.set("spark.executor.memory", "128m")
  //.set("spark.driver.memory", "128m")
  //.set("spark.local.dir", "/Users/yxl/data/spark.dir")
  val sc = new SparkContext(sparkConf)

  sc.makeRDD(Seq((1, "a"), (1, "b"), (2, "c")))
    .mapPartitionsWithIndex { (p, it) =>
    val outputs = new MultiWriter(p.toString)
    for ((k, v) <- it) {
      outputs.write(k.toString, v)
    }
    outputs.close
    Nil.iterator
  }.foreach((x: Nothing) => ()) // To trigger the job.

  // This one is Local, but you could write one for HDFS
  class MultiWriter(suffix: String) {
    private val writers = collection.mutable.Map[String, java.io.PrintWriter]()
    def write(key: String, value: Any) = {
      if (!writers.contains(key)) {
        val f = new java.io.File("output/" + key + "/" + suffix)
        f.getParentFile.mkdirs
        writers(key) = new java.io.PrintWriter(f)
      }
      writers(key).println(value)
    }
    def close = writers.values.foreach(_.close)
  }

}
