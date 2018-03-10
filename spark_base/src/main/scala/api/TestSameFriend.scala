package api

import base.BaseSession

import scala.collection.mutable.ListBuffer

/**

A:B,C,D,F,E,O
B:A,C,E,K
C:F,A,D,I

输出
A-B  C E
A-C  F D
B-C  A


  */
object TestSameFriend extends BaseSession {
    override protected val appName: String = "TestSameFriend"

    def main(args: Array[String]): Unit = {

        val data = spark.sparkContext.parallelize(
            Seq(
                ("A",List("B","C","D","F","E","O")),
                ("B",List("A","C","E","K")),
                ("C",List("F","A","D","I"))
            )
        )

        val friends = data
        .map(x => {
            x._2.map(y => {(y, x._1)})
        })
        .flatMap(x => x)
        .groupByKey()
        .filter(x => x._2.size > 1)
        .map(x => {
            val listBuffer = ListBuffer[String]()
            x._2.copyToBuffer(listBuffer)
            val sortedList = listBuffer.sortWith((x1,x2) => x1.compareTo(x2) < 0)
            sortedList.map(item1 => {
                val last = sortedList.dropWhile(item2 => item1.compareTo(item2) >= 0)
                last.map(item2 => {
                    ((item1,item2),x._1)
                })
            }).toList.flatten
        })
        .flatMap(x => x)
        .groupByKey()

        println(friends.collect().mkString(","))


    }
}
