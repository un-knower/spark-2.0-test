package main

import java.net.InetSocketAddress

/**
  * Created by yxl on 2018/2/1.
  */
object TestInetAddress {

    def main(args: Array[String]): Unit = {
        val preResolveHost = System.nanoTime
        val resolvedAddress = new InetSocketAddress("xxx.com.cn", 10000)
        val hostResolveTimeMs = (System.nanoTime - preResolveHost) / 1000000

        print(hostResolveTimeMs)

        print(resolvedAddress)
    }

}
