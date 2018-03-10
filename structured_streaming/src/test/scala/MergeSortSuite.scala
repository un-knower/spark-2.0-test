import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.{BufferedIterator, Iterator, mutable}

/**
  * Created by yxl on 2018/3/8.
  */
class MergeSortSuite extends FunSuite with Matchers with BeforeAndAfter {

    test("priority queue"){
        val queue = new mutable.PriorityQueue[Int]()

        queue.enqueue(5)
        queue.enqueue(3)
        queue.enqueue(6)

        println(queue.clone().dequeueAll)

    }

    test("merge sort") {

        val list1 = List(0, 5, 11, 18)
        val list2 = List(4, 7, 9, 14)
        val list3 = List(6, 10, 12, 17)

        val iterators = Seq(list1.iterator, list2.iterator, list3.iterator)

        val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)

        val heap = new mutable.PriorityQueue[BufferedIterator[Int]]()(new Ordering[BufferedIterator[Int]] {
            override def compare(x: BufferedIterator[Int], y: BufferedIterator[Int]): Int = {
                // PriorityQueue 是按照从大到小，实际中是判断全局最小的所以 -1
                -1 * (x.head - y.head)
            }
        })

        heap.enqueue(bufferedIters: _*)

        val iterator = new Iterator[Int] {
            override def hasNext: Boolean = !heap.isEmpty
            override def next(): Int = {
                if (!hasNext) {
                    throw new NoSuchElementException
                }
                val firstBuf = heap.dequeue()
                val item = firstBuf.next()
                if (firstBuf.hasNext) {
                    heap.enqueue(firstBuf)
                }
                item
            }
        }
        val s = iterator.mkString(",")
        println(s)
    }



}
