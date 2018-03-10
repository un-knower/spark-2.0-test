package yarn

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}
import scala.collection.JavaConverters._
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.generic.Growable

/**
 * Created by yxl on 16/5/18.
 */


/**
 * 直接 copy BoundedPriorityQueue
 * @param maxSize
 * @param ord
 * @tparam A
 */
class MyPriorityQueue [A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: A): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {
      maybeReplaceLowest(elem)
    }
    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear() { underlying.clear() }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}


object TestTopN {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestTopN")
      .setMaster("local[4]")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf);

    val scores = sc.parallelize(Array(
      ("a", 1),
      ("a", 2),
      ("a", 3),
      ("b", 3),
      ("b", 1),
      ("a", 4),
      ("b", 4),
      ("b", 2)
    ))


    // max(2)
    scores.mapValues(p => (p, p)).reduceByKey((u, v) => {
      val values = List(u._1, u._2, v._1, v._2).sorted(Ordering[Int].reverse).distinct
      if (values.size > 1) (values(0), values(1))
      else (values(0), values(0))
    })

    // max(2) 可能 OOM
    scores.groupByKey().mapValues(_.toSeq.distinct.sorted(Ordering[Int].reverse).take(2)).foreach(println(_))

    // (key,max(1))
    scores.reduceByKey((u,v) => {
         if(u > v){ u }else{ v }
    })


    val seqOp = (queue:MyPriorityQueue[Int],item:Int) => queue.+=(item)

    val combOp = (queue1:MyPriorityQueue[Int],queue2:MyPriorityQueue[Int]) => queue1.++=(queue2)

    // (key,max(n))
    scores.aggregateByKey(new MyPriorityQueue[Int](2)(Ordering[Int]))(seqOp,combOp).foreach(println(_))

  }


}
