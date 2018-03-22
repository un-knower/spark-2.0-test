package example

import base.StructuredBase

/**
  * Created by yxl on 2018/3/21.
  */
object ContinuousProcessing extends StructuredBase {
    override protected val appName: String = "ContinuousProcessing"


    def main(args: Array[String]): Unit = {

        import spark.implicits._

        super.addListener(false)

        val socket = super.socketData



    }
}
