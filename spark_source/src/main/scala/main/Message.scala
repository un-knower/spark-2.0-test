package main

/**
  * Created by yxl on 17/6/8.
  */
trait Message {

}

case class HelloMessage(msg:String) extends Message

case object BackMessage extends Message