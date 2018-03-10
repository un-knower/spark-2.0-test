package main

/**
  * Created by yxl on 2018/2/10.
  */

class SimplePartialFunction {


    def simple(base:Int): PartialFunction[String,Int] = {
        case x => base + x.toInt
    }

}

object TestPartialFunction {
    def main(args: Array[String]): Unit = {

        val simplePartialFunction = new SimplePartialFunction()

        val base = 10

        val rs = simplePartialFunction.simple(base).apply("2")

        println(rs)

    }
}
