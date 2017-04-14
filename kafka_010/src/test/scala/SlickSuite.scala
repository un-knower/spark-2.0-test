import org.scalatest.{BeforeAndAfter, FunSuite, Matchers, FlatSpec}
import slick.{Coffees, Suppliers}
import slick.driver.MySQLDriver.api._

/**
  * Created by yxl on 17/1/18.
  */
class SlickSuite extends FunSuite with Matchers with BeforeAndAfter {

    val db = Database.forConfig("db_test")

    val suppliers = TableQuery[Suppliers]

    val coffees = TableQuery[Coffees]

    after {
        db.close()
    }


    test("create schema") {
        val create = DBIO.seq(suppliers.schema.create,
            coffees.schema.create)

    }

}
