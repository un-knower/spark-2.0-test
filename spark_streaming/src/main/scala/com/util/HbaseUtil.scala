package com.util

import java.io.IOException

import com.common.Log
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by yxl on 17/5/9.
  */

class HbaseUtil(connection:Connection) extends Serializable with Log  {

  def createTable(tableName: String, columnFamilies: Seq[String]): Boolean = {
    val table = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    if (!admin.tableExists(table) && !admin.isTableAvailable(table)) {
      val tableDesc = new HTableDescriptor(table)

      columnFamilies foreach { family =>
        tableDesc.addFamily(new HColumnDescriptor(family))
      }
      try {
        admin.createTable(tableDesc)
        true
      } catch {
        case ex: IOException => {
          log.error(ex)
          false
        }
      }
    } else {
      true
    }
  }

  def dropTable(tableName:String):Boolean = {
    val table = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    try{
      admin.disableTable(table)
      admin.deleteTable(table)
      true
    }catch{
      case ex:IOException => {
        log.error(ex)
        false
      }
    }
  }

  /**
    * 指定版本 put
    * @param tableName
    * @param rowKey
    * @param columnFamily
    * @param columnQualifier
    * @param version
    * @param value
    */
  def putByVersion(tableName:String,rowKey:String,columnFamily:String,
                   columnQualifier:String,version:Int,value:String): Boolean ={
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier),
                  version,Bytes.toBytes(value))
    try{
      table.put(put)
      true
    }catch{
      case ex:Exception => {
        log.error(ex)
        false
      }
    }
  }


  def closeConnection  = {
    try {
      connection.close()
    } catch {
      case ex: Exception => {
        log.error(ex)
      }
    }

  }


}

object HbaseUtil extends Log {

  def apply(conf: Configuration) = {
    val connection = ConnectionFactory.createConnection(conf)
     new HbaseUtil(connection)
  }

  def apply() = {
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    new HbaseUtil(connection)
  }
}

