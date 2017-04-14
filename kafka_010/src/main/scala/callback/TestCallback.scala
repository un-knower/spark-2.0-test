package callback


import java.util.concurrent.TimeUnit

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import concurrent.ExecutionContext.Implicits.global

/**
  * Created by yxl on 16/12/8.
  */
object TestCallback {

    def main(args: Array[String]) {

        def producerCallbackWithPromise(promise: Promise[String]): CallBack = {
            producerCallback(result => {
                // println("producerCallbackWithPromise:" + result)
                println("producerCallbackWithPromise")
                promise.complete(result)
                println("done producerCallbackWithPromise")
            })
        }

        def producerCallback(callback: Try[String] => Unit): CallBack = {
            new CallBack {
                override def onComplete(msg: String): Unit = {
                    val result = {
                        if (msg == null) {
                            Failure(new Exception("exception"))
                        }
                        else {
                            Success(msg)
                        }
                    }
                    callback(result)
                }
            }
        }

        def wrapSend(msg: String)(callback: Try[String] => Unit): Unit = {
            val s = new Send()
            s.send(msg, producerCallback(callback))
        }

        def wrapSendAsync(msg: String): Future[String] = {
            val promise = Promise[String]()
            val s = new Send()
            s.send(msg, producerCallbackWithPromise(promise))
            val f = promise.future
            //println(f)
            f
        }

        //		wrapSend("hello")((result: Try[String]) => {
        //			println("wrap:" + result.get)
        //		})
        val f = Future {
            wrapSendAsync("async").onComplete((result: Try[String]) => {
                //			   result.get match {
                //				   case Success(str) => println("ok")
                //				   case Failure(exception) => println("exception:" + exception)
                //				   case _ => println("o")
                //			   }
                println("result:" + result)

            })

        }

        f.onComplete({
            case Success(str) =>
        })


        def fun: PartialFunction[Unit, Unit] = {
            case result => println(1)
        }

        f.onSuccess(fun)

        TimeUnit.SECONDS.sleep(10)
    }

}
