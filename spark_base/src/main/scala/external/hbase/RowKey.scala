package external.hbase

import java.security.MessageDigest

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by yxl on 2018/4/24.
  */
object RowKey {

    def hash(key:String): Array[Byte] = {
        val bytes = MessageDigest.getInstance("MD5").digest(key.getBytes)
        val rowKey = Bytes.add(bytes, Bytes.toBytes(key))
        rowKey
    }

    def get(rowKey:Array[Byte]):String = {
        Bytes.toString(rowKey,16)
    }


    def main(args: Array[String]): Unit = {

        val rowKey = RowKey.hash("20000")

        val key = RowKey.get(rowKey)
        println(key)
    }

}
