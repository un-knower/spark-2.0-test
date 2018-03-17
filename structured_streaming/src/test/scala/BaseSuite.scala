import java.util.Random

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import util.DateUtil

import scala.util.Try

/**
  * Created by yxl on 2018/3/7.
  */
class BaseSuite extends FunSuite with Matchers with BeforeAndAfter {

    test("base try"){
        val columns = Array(0,1,2)
        val time = Try(Some(columns(2))).getOrElse(None)
        println(time)
    }

    test("current"){
        val s = DateUtil.CURRENT
        println(s)
    }


}
