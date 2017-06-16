package main

import org.apache.spark.SparkConf
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.rpc.{RpcEnvConfig, RpcAddress}

/**
  * Created by yxl on 17/6/7.
  */

object LocalEnv {

    def main(args: Array[String]) {
        val conf = new SparkConf()
        val config = RpcEnvConfig(conf, "remote","127.0.0.1",0,clientMode = true)
        val anotherEnv = new NettyRpcEnvFactory().create(config)
        val address = RpcAddress("127.0.0.1",10086)
        val rpcEndpointRef = anotherEnv.setupEndpointRef(address, "printEndpoint")
        val msg = rpcEndpointRef.askWithRetry[Message](HelloMessage("hello"))
        println(msg)
    }

}
