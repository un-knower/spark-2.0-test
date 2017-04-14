package slick

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import slick.driver.MySQLDriver.api._

/**
  * Created by yxl on 17/1/18.
  */
object HelloSlick {


    def main(args: Array[String]): Unit = {

        val db = Database.forConfig("db_test")

        println(db)

        val suppliers = TableQuery[Suppliers]

        val coffees = TableQuery[Coffees]

        val create = DBIO.seq(suppliers.schema.create,
            coffees.schema.create)

        val createFuture = db.run(create)

        val code = Await.result(createFuture, 1 minute)

        println(code)

    }

}
