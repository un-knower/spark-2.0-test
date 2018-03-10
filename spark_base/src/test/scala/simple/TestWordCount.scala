package simple

/**
  * Created by yxl on 2018/2/24.
  */
object TestWordCount {
    def main(args: Array[String]): Unit = {

        val array = Array("hello", "world", "hello", "hello")

        val result = array.map(x => { (x,1)}).groupBy(x => x._1).map(x => {
            (x._1, x._2.foldLeft(0)((r, i) => r + i._2))
        }).toList.sortBy(x=> {x._2}).reverse

        println(result)

        val path = System.getProperty("java.io.tmpdir")
        println(path)

        val expath = System.getenv("SPARK_EXECUTOR_DIRS")

        println(expath != null)
    }
}
