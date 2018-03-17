package com.util

import java.sql._
import java.util.{Date, Properties}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

/**
  * Created by yxl on 2018/3/13.
  */
object DBUtil extends Logging{

    def insertTable(connection: Connection, map: Map[String, Any], tableName: String): Int = {
        val columns = map.filter( x => x.productElement(1) != null).keys.toSeq
        val values = columns.map(column => "?")
        val sqlBuffer = new StringBuffer()
        sqlBuffer.append("insert into ")
        .append(tableName)
        .append(columns.map(column => "`" + column + "`").mkString("(", ",", ")"))
        .append(" values").append(values.mkString("(", ",", ")"))
        val sql = sqlBuffer.toString
        val statement = connection.prepareStatement(sql)
        columns.zip(Stream.from(1)).map(item => {
            item match {
                case (column, index) => {
                    val columnValue = map.get(column)
                    setStatementParam(statement, index, columnValue.get)
                }
                case _ => throw new Exception(s"列名格式 $columns")
            }
        })
        logger.info(s"$sql 参数:${map.mkString(",")}")
        statement.executeUpdate()
    }

    def setStatementParam(statement: PreparedStatement, index: Int, columnValue: Any): Unit = {
        columnValue match {
            case v: String => statement.setString(index, v)
            case v: Integer => statement.setInt(index, v.toString.toInt)
            case v: Double => statement.setDouble(index, v.toString.toDouble)
            case v: Long => statement.setLong(index, v.toString.toLong)
            case v: Date =>
            {   val timestamp =  new Timestamp(v.getTime)
                statement.setTimestamp(index, timestamp) }
            case v: Float => statement.setFloat(index, v.toString.toFloat)
            case v: Boolean => statement.setBoolean(index, v)
            case null => statement.setNull(index,Types.NULL)
            case _ => throw new Exception(s"未知类型 $columnValue")
        }

    }

    def runSQL(connection: Connection, sql: String, param : Seq[Any]): Int = {
        val statement = connection.prepareStatement(sql)
        param.zip(Stream.from(1)).map(item => {
            item match {
                case (column, index) => {
                    setStatementParam(statement, index,column)
                }
                case _ => throw new Exception(s"列名格式 $item")
            }
        }
        )
        logger.info(s"运行 SQL:$sql 参数: ${param.mkString(",")}")
        statement.executeUpdate()
    }


    def runQuerySQLCount(connection: Connection, sql: String, param : Seq[Any]): Int = {
        val statement = connection.prepareStatement(sql)
        param.zip(Stream.from(1)).map(item => {
            item match {
                case (column, index) => {
                    setStatementParam(statement, index,column)
                }
                case _ => throw new Exception(s"列名格式 $param")
            }
        }
        )
        logger.info(s"运行 SQL:$sql 参数: ${param.mkString(",")}")
        val resultSet = statement.executeQuery()
        resultSet.last()
        resultSet.getRow()
    }


    def createMySQLConnectionFactory(url: String, properties: Properties): Connection = {
        val userSpecifiedDriverClass = properties.getProperty("driver")
        Class.forName(userSpecifiedDriverClass)
        DriverManager.getConnection(url, properties)
    }

    def createMySQLConnectionFactory(url:String,userName:String,password:String):Connection = {
        val properties = new Properties()
        properties.setProperty("user", userName)
        properties.setProperty("password", password)
        properties.setProperty("useUnicode", "true")
        properties.setProperty("characterEncoding", "UTF-8")
        properties.setProperty("driver","com.mysql.jdbc.Driver")
        createMySQLConnectionFactory(url,properties)
    }

}
