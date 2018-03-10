package yarn

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yxl on 16/8/11.
  */
object Test1 extends App {
  val sparkConf = new SparkConf().setAppName("TestSecondSort")
  .setMaster("local[2]")
  .set("spark.cores.max", "1")
  //.set("spark.executor.memory", "128m")
  //.set("spark.driver.memory", "128m")
  .set("spark.local.dir", "/Users/yxl/data/spark.dir")


  val sc = new SparkContext(sparkConf);

  val arr = Array(("a", "x"), ("a", "y"), ("a", "y"), ("a", "x"), ("a", "x"),
    ("a", "y"), ("a", "x"), ("b", "x"), ("b", "y"), ("b", "y"), ("b", "x"), ("c", "x"), ("c", "x"))

  def compressRecursive[A](ls: List[A]): List[A] = ls match {
    case Nil       => Nil
    case h :: tail => h :: compressRecursive(tail.dropWhile(_ == h))
  }

  val rdd = sc.parallelize(arr).groupByKey().mapValues(it => it.toList).mapValues(
    list => {
      compressRecursive(list)
    }
  ).mapValues(list => list.size -1 )

  rdd.foreach(println(_))

}
