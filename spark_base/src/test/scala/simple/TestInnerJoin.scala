package simple

import scala.collection.mutable.ListBuffer

/**
  * Created by yxl on 2018/3/10.
  */
object TestInnerJoin {

    def main(args: Array[String]): Unit = {

        val listBuffer = ListBuffer[String]()

        listBuffer.append(List("A","C", "B","E","D"):_*)

        val sortList = listBuffer.sortWith((x1,x2) => x1.compareTo(x2) < 0)
        val r = sortList.map(item1 => {
            val last = sortList.dropWhile(item2 => item1.compareTo(item2) >= 0)
            println(last)
            last.map(item2 => {
                (item1,item2)
            })
        }).toList.flatten

        println(r)

    }

}
