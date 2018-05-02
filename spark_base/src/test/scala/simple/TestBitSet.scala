package simple

import scala.collection.mutable

/**
  * Created by yxl on 2018/3/23.
  */
object TestBitSet {
    def main(args: Array[String]): Unit = {

        val memory = Runtime.getRuntime.maxMemory()
        println(memory)
        val bitSet = new mutable.BitSet()
        (0 until 2000000000).map(x =>{
            bitSet.add(x)
        })

       val spent = Runtime.getRuntime.maxMemory() - memory
        println(spent)
       println(bitSet.exists( _ == 1))

    }
}
