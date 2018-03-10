package yarn

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/9/22.
 */




object TestSortKey {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestSortKey")
    .setMaster("local[*]")
    .set("spark.cores.max", "1").set("spark.executor.memory", "128m")
    .set("spark.driver.memory", "128m")
    .set("spark.local.dir", "/Users/yxl/data/spark.dir")


    val sc = new SparkContext(sparkConf);

    val rdd = sc.textFile("/Users/yxl/data/sortKey.txt")

    case class OrderedKey(k1:String,k2:String)

    implicit val sortOrderKey = new Ordering[OrderedKey] {
       override def compare(x: OrderedKey, y: OrderedKey) = {
         if(x.k1.compare(y.k1) > 0){
            1 ;
         }else if(x.k1.compare(y.k1) == 0){
           if(x.k2.compare(y.k2) > 0){
              -1 ;
           }else{
              1 ;
           }
         }else{
            -1
         }
       }
    }

    rdd.filter(line => {
      if(StringUtils.isEmpty(line)){
        false
      }else{
        true
      }
    }).map(line => {
        val array = line.split(",")
      (OrderedKey(array(0),array(1)),array(2))
    }).repartition(1).sortByKey(true).foreach(println(_))

    sc.stop()
  }

}
