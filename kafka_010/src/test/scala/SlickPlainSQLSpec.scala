import org.scalatest.Matchers
import slick.jdbc.GetResult
import slick.{Coffee, DBServer}
import slick.driver.MySQLDriver.api._
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import tagged.SlickPlainSQLTag

/**
  * Created by yxl on 17/1/19.
  */
class SlickPlainSQLSpec extends UnitSpec with Matchers {

    behavior of "slick query with plain sql"


    def withDatabase(testCode: (Database) => Any) {
        val db = DBServer.getDB() // create the fixture
        try {
            testCode(db) // "loan" the fixture to the test
        }
        finally {
            DBServer.closeDB(db)
        } // clean up the fixture
    }

    it should "filter cof_name not empty" taggedAs (SlickPlainSQLTag) in {
        withDatabase(db => {
            val coffeeName = "Colombian_Decaf"
            val querySQL = sql"select cof_name from t_coffees where cof_name = ${coffeeName}".as[(String)].headOption
            val futureCoffee = db.run(querySQL)
            val coffee = Await.result(futureCoffee, Duration.Inf)
            assert(coffee.contains(coffeeName))
        })
    }

    it should "count gt 0" taggedAs (SlickPlainSQLTag) in {
        withDatabase(db => {
            val querySQL = sql"select count(cof_name) from t_coffees".as[(Int)].headOption
            val futureCount = db.run(querySQL)
            val count = Await.result(futureCount, Duration.Inf)
            assert(count.get >= 0)
        })
    }

    ignore should "insert into t_coffees" taggedAs (SlickPlainSQLTag) in {
        withDatabase(db => {
            val c = Coffee("French_Roast_Decaf", 49, 9.99, 0, 0)
            def insert(coffee: Coffee): DBIO[Int] = {
                val insertSQL = sqlu"insert into t_coffees values (${c.cofName}, ${c.supId}, ${c.price}, ${c.sales}, ${c.total})"
                insertSQL
            }
            //val inserts = DBIO.sequence(Seq(c).map(insert))
            val futureCode = db.run(insert(c))
            val code = Await.result(futureCode, Duration.Inf)
            assert(code == 1)
        })
    }

    it should "query return Coffee case class" taggedAs (SlickPlainSQLTag) in {
        withDatabase(db => {
            implicit val getCoffeeResult = GetResult(r => Coffee(r.nextString(),
                r.nextInt(), r.nextDouble(), r.nextInt(), r.nextInt()))

            val querySQL = sql"select cof_name,sup_id,price,sales,total from t_coffees".as[Coffee]
            val futureList = db.run(querySQL)
            val list = Await.result(futureList, Duration.Inf)
            // println(list)
            assert(list.nonEmpty)
        })
    }

}
