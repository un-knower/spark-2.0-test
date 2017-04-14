package basic

import java.util
import java.util.Map.Entry

/**
  * Created by yxl on 16/12/28.
  */
object TestLinkedHashMap {

    def main(args: Array[String]): Unit = {

        val map = new util.LinkedHashMap[String, String]() {
            override def removeEldestEntry(eldest: Entry[String, String]): Boolean = {
                if (this.size() > 10) {
                    true
                } else {
                    false
                }
            }
        }

        for (i <- (0 until 100)) {
            map.put(i.toString, i.toString)
        }

        print(map)


    }

}
