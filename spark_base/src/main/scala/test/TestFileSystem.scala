package test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by yxl on 16/12/2.
  */
object TestFileSystem {


  def main(args:Array[String]): Unit ={
    // 判断本地路径，LocalFileSystem
    val path = new Path("file:///Users/yxl/data/spark.dir/checkpoint/network/offsets")
    val fs = path.getFileSystem(new Configuration())
    println(fs.exists(path))

  }

}
