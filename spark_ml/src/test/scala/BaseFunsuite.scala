/**
  * Created by yxl on 2017/11/29.
  */
class BaseFunsuite {


}

object BaseFunsuite {

    def main(args: Array[String]): Unit = {
        val a = System.nanoTime()
        println( a / 1000000000.0)
        println(a/ 1e9)

        val weights = Array(0.3,0.4,0.2)
        val sum = weights.sum

        val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)

        normalizedCumWeights.sliding(2).foreach(x => println(x.mkString(",")))

        println(normalizedCumWeights.mkString(","))
    }
}