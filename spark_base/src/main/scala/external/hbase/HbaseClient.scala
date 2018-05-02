package external.hbase

import collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.parquet.filter2.predicate.Operators
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by yxl on 2018/4/13.
  */
class HbaseClient extends FunSuite with Matchers with BeforeAndAfter  {

    val conf = HBaseConfiguration.create()

    val connection = ConnectionFactory.createConnection(conf)


    val baseKey = 100000000

    test("print hbase conf") {
        println(conf)
    }

    test("print hbase connection"){
        println(connection.getClass)
    }

    test("simple put"){

        val table = connection.getTable(TableName.valueOf("test"))
        println(table)

        val cf = Bytes.toBytes("cf")
        val qualifier = Bytes.toBytes("c")

        val put = new Put(Bytes.toBytes("row3"))
        val value = Bytes.toBytes("value3")
        put.addColumn(cf, qualifier, value)

        table.put(put)
    }

    test("simple multi put"){
        val table = connection.getTable(TableName.valueOf("test"))

        val puts = (1 until  100).map(i =>{
            val cf1 = Bytes.toBytes("cf1")
            val cf2 = Bytes.toBytes("cf2")
            val qualifier1 = Bytes.toBytes("c1")
            val qualifier2 = Bytes.toBytes("c2")
            val value = Bytes.toBytes("value" + i)
            val put = new Put(RowKey.hash(i.toString))
            put.addColumn(cf1, qualifier1, value)
            put.addColumn(cf2, qualifier1, value)
            put.addColumn(cf1, qualifier2, value)
            put.addColumn(cf2, qualifier2, value)
            put
        }).toList

        table.put(puts)
    }

    test("bulk load"){
        val bulkLoader = new LoadIncrementalHFiles(conf)
        val table = connection.getTable(TableName.valueOf("test"))
        val path = new Path("")
        bulkLoader.doBulkLoad(path, table.asInstanceOf[HTable])
    }

    test("get"){
        val table = connection.getTable(TableName.valueOf("test"))
        val rowKey = RowKey.hash(101.toString)

        val get = new Get(rowKey)

        val result = table.get(get)

        val row = RowKey.get(result.getRow)

        val value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("c1"))

        println(row , Bytes.toString(value))
    }

    def printResultScanner(resultScanner:ResultScanner): Unit ={
        for(rs <- resultScanner){
            val cells = rs.listCells()
            cells.map(cell => {
                val row = CellUtil.cloneRow(cell)
                val columnFamily = CellUtil.cloneFamily(cell)
                val qualifier = CellUtil.cloneQualifier(cell)
                val value = CellUtil.cloneValue(cell)
                val seq = Seq(RowKey.get(row),Bytes.toString(columnFamily),
                    Bytes.toString(qualifier),Bytes.toString(value))
                println(seq.mkString(","))
            })
        }
        resultScanner.close()
    }

    test("simple scan"){
        val table = connection.getTable(TableName.valueOf("test"))
        val startKey = RowKey.hash(1.toString)
        val endKey = RowKey.hash(30.toString)
        val scan = new Scan()
        //scan.setCaching(2) // 每次取记录数，一次RPC 返回多少条数据
        //scan.setBatch(2)
        scan.setStartRow(startKey)
        scan.setStopRow(endKey)
        val resultScanner = table.getScanner(scan)
        println(resultScanner.size)
    }

    test("rowkey filter scan"){

        val table = connection.getTable(TableName.valueOf("test"))
        val startKey = RowKey.hash(1.toString)
        val endKey = RowKey.hash(30.toString)

        val scan = new Scan()

        val filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
            new BinaryComparator(endKey));
        scan.setFilter(filter)
        val resultScanner = table.getScanner(scan)
        printResultScanner(resultScanner)

    }

    test("region"){

        val regionLocator = connection.getRegionLocator(TableName.valueOf("test"))

        val keys = regionLocator.getStartEndKeys

        val hbaseRegions = keys.getFirst.zip(keys.getSecond)
        .zipWithIndex
        .map(x => {
            val seq = Seq(x._2, x._1._1, x._1._2, regionLocator.getRegionLocation(x._1._1).getHostname)
            println(seq.mkString(","))
        })

        regionLocator.close()
    }

    test("create table"){
        val admin = connection.getAdmin()
        val tableName = TableName.valueOf("test")
        val cf1 = new HColumnDescriptor("cf1")
        val cf2 = new HColumnDescriptor("cf2")
        val descriptor = new HTableDescriptor(tableName)
        descriptor.addFamily(cf1)
        descriptor.addFamily(cf2)
        admin.createTable(descriptor)
    }

    test("empty table"){
        val admin = connection.getAdmin()

        val tableName = TableName.valueOf("shc_test")

        admin.disableTable(tableName)
        admin.truncateTable(tableName,false)
    }

    test("delete table"){
        val admin = connection.getAdmin()
        val tableName = TableName.valueOf("test")
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
    }

    test("meta"){
        val table = connection.getTable(TableName.valueOf("hbase:meta"))
        val scan = new Scan()
        val resultScanner = table.getScanner(scan)
        printResultScanner(resultScanner)
    }

    def printResultScanner(resultScanner:ResultScanner, printf:(Array[Byte],Array[Byte],Array[Byte],Array[Byte]) => Unit): Unit ={
        for(rs <- resultScanner){
            val cells = rs.listCells()
            cells.foreach(cell => {
                val row = CellUtil.cloneRow(cell)
                val columnFamily = CellUtil.cloneFamily(cell)
                val qualifier = CellUtil.cloneQualifier(cell)
                val value = CellUtil.cloneValue(cell)
                printf(row,columnFamily,qualifier,value)
            })
        }
        resultScanner.close()
    }

    test("row filter"){
        val table = connection.getTable(TableName.valueOf("shc_test"))
        val  rowFilter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(2)))

        val scan = new Scan()
        scan.setFilter(rowFilter)

        val scanner = table.getScanner(scan)
        printResultScanner(scanner, (row, columnFamily, qualifier, value) => {
            val qualifierStr = Bytes.toString(qualifier) ;
            val extractValue = qualifierStr match {
                case "name" => Bytes.toString(value)
                case "age" | "status" => Bytes.toInt(value)
            }
            println(Bytes.toInt(row), Bytes.toString(columnFamily), qualifierStr, extractValue)
        })
    }

    def printSHCTESTScanner(scanner:ResultScanner): Unit ={
        printResultScanner(scanner, (row, columnFamily, qualifier, value) => {
            val qualifierStr = Bytes.toString(qualifier) ;
            val extractValue = qualifierStr match {
                case "name" => Bytes.toString(value)
                case "age" | "status" => Bytes.toInt(value)
            }
            println(Bytes.toInt(row), Bytes.toString(columnFamily), qualifierStr, extractValue)
        })
    }

    test("column prefix filter"){
        val table = connection.getTable(TableName.valueOf("shc_test"))

        val columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("cf1"))

        val qualifierFilter1 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name")))
        val qualifierFilter2 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("age")))

        val qualifierFilterList = new FilterList(Operator.MUST_PASS_ONE)
        qualifierFilterList.addFilter(qualifierFilter1)
        qualifierFilterList.addFilter(qualifierFilter2)

        val scanFilterList = new FilterList(Operator.MUST_PASS_ALL)

        //scanFilterList.addFilter(columnPrefixFilter)
        scanFilterList.addFilter(qualifierFilterList)

        val scan = new Scan()
        scan.setFilter(scanFilterList)

        val scanner = table.getScanner(scan)
        printSHCTESTScanner(scanner)

    }

    test("single column value filter"){
        val table = connection.getTable(TableName.valueOf("shc_test"))

        val family = Bytes.toBytes("cf1")
        val qualifier = Bytes.toBytes("age")
        val value = Bytes.toBytes(15)
        val singleColumnValueFilter = new SingleColumnValueFilter(family,qualifier,
            CompareOp.GREATER_OR_EQUAL, new BinaryComparator(value))

        val qualifierFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("age")))

        val filterList = new FilterList()
        filterList.addFilter(singleColumnValueFilter)
        filterList.addFilter(qualifierFilter)

        val scan = new Scan()
        scan.setFilter(filterList)

        val scanner = table.getScanner(scan)
        printSHCTESTScanner(scanner)
    }

    test("scan spec column"){
        val table = connection.getTable(TableName.valueOf("shc_test"))

        val scan = new Scan()
        val family = Bytes.toBytes("cf1")
        val ageQualifier = Bytes.toBytes("age")
        val nameQualifier = Bytes.toBytes("name")

        scan.addColumn(family,ageQualifier)
        scan.addColumn(family,nameQualifier)

        val scanner = table.getScanner(scan)
        printSHCTESTScanner(scanner)

    }

}
