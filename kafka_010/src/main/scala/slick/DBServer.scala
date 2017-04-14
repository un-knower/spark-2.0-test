package slick

import slick.driver.MySQLDriver.api._

/**
  * Created by yxl on 17/1/19.
  */
object DBServer {

    def getDB(): Database = {
        val db = Database.forConfig("db_test")
        db
    }

    def getCoffeesQuery() = {
        val coffees = TableQuery[Coffees]
        coffees
    }


    def closeDB(db: Database) = {
        db.close()
    }

    def main(args: Array[String]) {
        println(getCoffeesQuery())
    }
}
