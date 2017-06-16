package main

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnvConfig}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory

/**
  * Created by yxl on 17/6/7.
  */


object RemoteEnv {
    def main(args: Array[String]) {
        val conf = new SparkConf()
        val config = RpcEnvConfig(conf, "RemoteEnv", "127.0.0.1", 10086, clientMode = false)
        val remoteEnv = new NettyRpcEnvFactory().create(config)
        println(s"address:${remoteEnv.address}")
        remoteEnv.setupEndpoint("printEndpoint",new RpcEndpoint{
            override val rpcEnv = remoteEnv
            override def receive: PartialFunction[Any, Unit] = {
                case msg:String => {
                     println(msg)
                }
            }
            override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
                case msg:String => {
                     println(s"receive:$msg")
                     context.reply("world")
                }
                case msg:HelloMessage => {
                    println(s"receive:${msg.msg}")
                    context.reply(BackMessage)
                }
            }
        })
        remoteEnv.awaitTermination()
    }
}
