package api

import base.BaseSession

import scala.collection.mutable.ListBuffer

/**
  *  a -> b,c
  *  b -> a,e
  *  e -> b
  *  f -> g
  *
  * 输出 (a,e) (b,c), (a,f)
  * */

object TestSecondFriend extends BaseSession {


    override protected val appName: String = "TestSecondFriend"

    def main(args: Array[String]): Unit = {

        val rdd = spark.sparkContext.parallelize(
            Seq(("a",List("b","c")),("b",List("a","e","f")))
        )

        val friends = rdd
        .map(x => {
            x._2.map(y => (x._1,y)) ++ x._2.map(y => (y,x._1))
        })
        .flatMap(x => x)
        .groupByKey()
        .mapValues(x => x.toSet)
        .filter(x => x._2.size > 1)
        .values
        .collect()

        println(s"friends:${friends.mkString(",")}")

        friends.map(friend =>{
            val buffer = ListBuffer[String]()
            buffer.append(friend.toSeq:_*)
            for(x <- friend ; y <- buffer.toList){
                if(x != y){
                    println(s"$x -> $y")
                }
            }
        })

    }

}
