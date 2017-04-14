package slick


import slick.driver.MySQLDriver.api._
import slick.lifted.{ProvenShape, ForeignKeyQuery}

/**
  * Created by yxl on 17/1/18.
  */


case class Supplier(id: Option[Int], name: String, street: String, city: String, state: String, zip: String)

// A Suppliers table with 6 columns: id, name, street, city, state, zip
class Suppliers(tag: Tag)
    extends Table[Supplier](tag, "t_suppliers") {

    // This is the primary key column:
    def id: Rep[Int] = column[Int]("SUP_ID", O.PrimaryKey, O.AutoInc)

    def name: Rep[String] = column[String]("SUP_NAME")

    def street: Rep[String] = column[String]("STREET")

    def city: Rep[String] = column[String]("CITY")

    def state: Rep[String] = column[String]("STATE")

    def zip: Rep[String] = column[String]("ZIP")

    // Every table needs a * projection with the same type as the table's type parameter
    def * = (id.?, name, street, city, state, zip) <>((Supplier.apply _).tupled, Supplier.unapply)
}

case class Coffee(cofName: String, supId: Int, price: Double, sales: Int, total: Int)

// A Coffees table with 5 columns: name, supplier id, price, sales, total
class Coffees(tag: Tag)
    extends Table[(String, Int, Double, Int, Int)](tag, "t_coffees") {

    def name: Rep[String] = column[String]("COF_NAME", O.PrimaryKey)

    def supID: Rep[Int] = column[Int]("SUP_ID")

    def price: Rep[Double] = column[Double]("PRICE")

    def sales: Rep[Int] = column[Int]("SALES")

    def total: Rep[Int] = column[Int]("TOTAL")

    def * : ProvenShape[(String, Int, Double, Int, Int)] =
        (name, supID, price, sales, total)
}
