package com.hash

import scala.collection.mutable

/**
  * Created by yxl on 2018/3/24.
  */
object SimpleHash {
    def main(args: Array[String]): Unit = {

        var array = new Array[Node](3)

        class Node(var key:String,var value:String,var next:Node){
            override def toString: String = {
                s"Node[${key},${value},${next}]"
            }
        }

        def hash(key:Any): Int = {
            var h = key.hashCode()
            h = h ^ (h >>> 16)
            h
        }

        def resize(size:Int): Unit = {
            val newArray = new Array[Node](size)
            for(i <- 0 until array.length){
                newArray(i) = array(i)
            }
        }

        def put(key:String,value:String): Unit ={
            val length = array.length
            val position = (length -1) & hash(key)
            println(s"position:${position}")
            if(array(position) == null){
                val node = new Node(key,value,null)
                array(position) = node
            }else{
                val newNode = new Node(key,value,null)
                var node = array(position)
                var flag = true
                while(flag){
                    if(node.key.equals(key)){
                        node.value = value
                        flag = false
                    }
                    if(node.next == null){
                        node.next = newNode
                        flag = false
                    }else{
                        node = node.next
                    }
                }

                resize(5)
            }
        }

        def get(key:String):String = {
            val length = array.length
            val position = (length -1) & hash(key)
            var item = array(position)
            if(item == null){
                null
            }else{
                while(true){
                    if(item.key.equals(key)){
                        return item.value
                    }else{
                        if(item.next == null){
                            return null
                        }else{
                            item = item.next
                        }
                    }
                }
               null
            }
        }

        put("hello","hello")
        put("hello1","hello1")
        put("hello2","hello2")
        put("hello3","hello3")
        put("hello4","hello4")
        put("hello5","hello5")

        println(array.mkString(","))

        val item1 = get("hello7")
        println(item1)

        put("hello7","hello7")
        println(get("hello7"))

        val map = new mutable.HashMap[String,String]()
        map.put("hello","hello")

    }
}
