import java.nio.charset.Charset

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by yxl on 2018/3/7.
  */
class UnicodeSuite extends FunSuite with Matchers with BeforeAndAfter  {


    test("iso8859"){

        val source = "张三"

        val array = source.getBytes(Charset.forName("ISO_8859_1"))

        val target = new String(array)

        val change = new String(target.getBytes(Charset.forName("utf-8")),
            Charset.forName("ISO_8859_1"))

        println(change)

    }


    test("utf-8"){
        val source = "张三"
        val array = source.getBytes(Charset.forName("utf-8"))
        val target = new String(array)

        val change = new String(target.getBytes(Charset.forName("ISO_8859_1")),
            Charset.forName("ISO_8859_1"))

        println(change)


    }

}
