package sql

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/7/22.
 */
object TestJdbc {

   def main(args:Array[String]) {

     val sparkConf = new SparkConf().setAppName("TestKafka")
       .setMaster("local[4]")
       .set("spark.cores.max","3").set("spark.executor.memory","128m")
       .set("spark.driver.memory","128m")
       .set("spark.local.dir","/Users/yxl/data/spark.dir")

     val sc = new SparkContext(sparkConf)
     val sqlContext = new SQLContext(sc)

     val url = "jdbc:mysql://localhost:3306/test?user=root&password=root"
     val table = "t_test"
     val predicates = Array("name='a'","name='b'","name='c'") // 相当于手动的分partition
     val properties = new Properties()
     val df = sqlContext.read.jdbc(url,table,predicates,properties)

     // 相当于有 3 个 partition a , b , c
     df.rdd.partitions.zipWithIndex.map(x => {
        x match {
          case (item,index) => println(index + " " + item)
        }
     })
     df.collect().foreach(println(_))

     df.registerTempTable("t_test")

     val rs = sqlContext.sql("select name,count(1) as t_count from t_test group by name").toDF()

     rs.collect().foreach(println(_))

     // 保存到 MySQL 中
     rs.write.mode(SaveMode.Overwrite).jdbc(url,"t_new",properties)

   }

}
