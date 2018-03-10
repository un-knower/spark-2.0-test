package encoder
import org.apache.spark.sql.catalyst.ScalaReflection.deserializerFor
import org.apache.spark.sql.catalyst.ScalaReflection.serializerFor
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.types.TimestampType

/**
  * Created by yxl on 2017/12/4.
  */
object TestExpressionEncoder {
    def main(args: Array[String]): Unit = {
        val timestampDeExpr = deserializerFor[java.sql.Timestamp]
        println(timestampDeExpr)
        println("-----")
        println(timestampDeExpr.numberedTreeString)

        println("*******")
        val boundRef = BoundReference(ordinal = 0, dataType = TimestampType, nullable = true)
        val timestampSerExpr = serializerFor[java.sql.Timestamp](boundRef)
        println(timestampSerExpr.numberedTreeString)


    }

}
