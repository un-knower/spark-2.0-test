package com.multi

import com.util.DBUtil

/**
  * Created by yxl on 2018/3/14.
  */
object Test {
    def main(args: Array[String]): Unit = {
//        val url = "jdbc:mysql://localhost:3306/test"
//        val connection = DBUtil.createMySQLConnectionFactory(url, "root","root")
//        println(connection)

        import scala.reflect.runtime.universe._

        def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
            case m: MethodSymbol if m.isCaseAccessor => m
        }.toList

        typeOf[Score].members


        println(classAccessors[Score].map(x => x.name).mkString(","))

    }
}
