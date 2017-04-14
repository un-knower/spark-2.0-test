import org.scalatest.BeforeAndAfter
import org.scalatest.tagobjects.Slow
import slick.{DBServer, Coffees, Suppliers}
import tagged.SlickTag
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import slick.driver.MySQLDriver.api._


/**
  * Created by yxl on 17/1/18.
  */
class SlickSpec extends UnitSpec with BeforeAndAfter {

    before {
        println("test before")
    }

    after {
        println("test after")
    }

    def fixture = new {
        val db = Database.forConfig("db_test")
        val suppliers = TableQuery[Suppliers]
        val coffees = TableQuery[Coffees]
    }

    "coffees table" should "query not empty" taggedAs(Slow, SlickTag) in {
        val f = fixture
        import f._
        val query = coffees.result
        val list = Await.result(db.run(query), 1 minute)
        db.close()
        list.size should be > 0
    }

    it should "insert then get name" taggedAs(Slow, SlickTag) in {
        val f = fixture
        import f._
        val values = ("Colombian_Decaf", 101, 8.99, 0, 0)
        val insert = coffees.insertOrUpdate(values)
        val query = coffees.filter(_.name === "Colombian_Decaf").map(_.name).result
        val futureName = db.run(insert.andThen(query))
        val name = Await.result(futureName, 1 minute)
        db.close()
        assert(name.head == "Colombian_Decaf")
    }

    def withDatabase(testCode: (Database) => Any) {
        val db = DBServer.getDB() // create the fixture
        try {
            testCode(db) // "loan" the fixture to the test
        }
        finally {
            DBServer.closeDB(db)
        } // clean up the fixture
    }

    it should "coffees query with loan fixture" taggedAs (SlickTag) in withDatabase(db => {
        val f = fixture
        val query = f.coffees.result
        val list = Await.result(db.run(query), 1 minute)
        assert(list.size >= 0)
    })


}
