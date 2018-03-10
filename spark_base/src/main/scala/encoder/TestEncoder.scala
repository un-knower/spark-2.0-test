package encoder

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
  *  测试Spark Encoder  object <=> internal row
  */


case class Person(id:Long, name:String)
object TestEncoder {
    def main(args: Array[String]): Unit = {

       val personEncoder = Encoders.product[Person]

        val schema = personEncoder.schema
        println(s"schema:$schema")

        val clsTag = personEncoder.clsTag
        println(s"clsTag:$clsTag")

        val personExprEncoder = personEncoder.asInstanceOf[ExpressionEncoder[Person]]

        val flat = personExprEncoder.flat
        println(s"flat:$flat")

        val ser =  personExprEncoder.serializer
        println(s"ser:$ser")

        val des = personExprEncoder.deserializer
        println(s"des:$des")

        val namedExpression =  personExprEncoder.namedExpressions
        println(s"namedExpression:$namedExpression")

        val jack = Person(0, "Jack")

        val row = personExprEncoder.toRow(jack)
        println(s"row:$row")

        val attrs = Seq(DslSymbol('id).long, DslSymbol('name).string)

        val jackReborn = personExprEncoder.resolveAndBind(attrs).fromRow(row)
        println(s"jackReborn:$jackReborn")

        println(jack == jackReborn)
    }

}
