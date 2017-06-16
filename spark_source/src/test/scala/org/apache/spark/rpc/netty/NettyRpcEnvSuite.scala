/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rpc.netty

import org.apache.spark._
import org.apache.spark.rpc._
import org.scalatest.concurrent.Eventually._

import scala.concurrent.duration._


class NettyRpcEnvSuite extends RpcEnvSuite {

    override def createRpcEnv(
                             conf: SparkConf,
                             name: String,
                             port: Int,
                             clientMode: Boolean = false): RpcEnv = {
        val config = RpcEnvConfig(conf, "test", "localhost", port,
            clientMode)
        new NettyRpcEnvFactory().create(config)
    }

    test("non-existent endpoint") {
        val uri = RpcEndpointAddress(env.address, "nonexist-endpoint").toString
        val e = intercept[SparkException] {
            env.setupEndpointRef(env.address, "nonexist-endpoint")
        }
        assert(e.getCause.isInstanceOf[RpcEndpointNotFoundException])
        assert(e.getCause.getMessage.contains(uri))
    }

    test("property of rpcenv"){

        println(Runtime.getRuntime.availableProcessors())
        println(s"address:${env.address}")

        val list = List(1,2,3,"seven").collect({case i:Int => i + 1})
        println(list)
    }

    test("send a message locally") {
        @volatile var message: String = null

        val rpcEndpointRef = env.setupEndpoint("send-locally", new RpcEndpoint {
            override val rpcEnv = env

            override def receive = {
                case msg: String => message = msg
            }

        })
        rpcEndpointRef.send("hello")
        eventually(timeout(5 seconds), interval(10 millis)) {
            assert("hello" === message)
        }
    }

    test("client send message to remote"){
        val anotherEnv = createRpcEnv(new SparkConf(), "remote",0, clientMode = true)
        val address = RpcAddress("127.0.0.1",10086)
        val rpcEndpointRef = anotherEnv.setupEndpointRef(address, "printEndpoint")
        val msg = rpcEndpointRef.askWithRetry[String]("hello")
        println(msg)
    }

    test("ask a message remotely") {
        env.setupEndpoint("ask-remotely", new RpcEndpoint {
            override val rpcEnv = env

            override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
                case msg: String =>
                   //  println(s"context:$context")
                    context.reply(msg)
            }
        })

        val anotherEnv = createRpcEnv(new SparkConf(), "remote",10087, clientMode = false)
        // Use anotherEnv to find out the RpcEndpointRef
        val rpcEndpointRef = anotherEnv.setupEndpointRef(RpcAddress("localhost",10086), "ask-remotely")
        try {
            val reply = rpcEndpointRef.askWithRetry[String]("hello")
            assert("hello" === reply)
        } finally {
            anotherEnv.shutdown()
            anotherEnv.awaitTermination()
        }
    }

}
