package product

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.combinator.JavaTokenParsers

/**
  * Created by yxl on 16/10/10.
  */

case class Operator(value: String)

case class Literal(value: String)

case class Where(left: Literal, operator: Operator, right: Literal)

object WhereExpr extends JavaTokenParsers {

    def expr: Parser[Where] = literal ~ operator ~ literal ^^ {
        case left ~ op ~ right => Where(left, op, right)
    }

    def literal: Parser[Literal] = "[\\w|\\-|^[\\u4E00-\\u9FA5]+$]*".r ^^ { x => Literal(x) }

    def operator: Parser[Operator] = "[>|<|>=|<=|!=]*".r ^^ { x => Operator(x) }

    def parse(text: String) = parseAll(expr, text)

}

object Mongo2Hive {

    val log = Logger.getLogger(this.getClass)

    val FROM = "--from"
    val TO = "--to"
    val COLUMNS = "--columns"
    val WHERE = "--where"
    val EXCLUDECOLUMNS = "--exclude-columns"
    val PARTITION = "--partition"

    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            System.err.println("Usage: Mongo2Hive  --form xxx --to xxx --columns xxx,xxx --where xxx>123 --exclude-columns xxx,xxx")
            System.exit(1)
        }

        var mongoCollection = ""
        var hiveTable = ""
        var columns = ""
        var where = ""
        var excludeColumns = ""
        var partition = ""

        for (i <- (0 until args.size)) {
            if (i % 2 == 0) {
                val name = args(i).trim()
                val value = args(i + 1).trim()
                name match {
                    case FROM => mongoCollection = value
                    case TO => hiveTable = value
                    case COLUMNS => columns = value
                    case WHERE => where = value
                    case EXCLUDECOLUMNS => excludeColumns = value
                    case PARTITION => partition = value
                }
            }
        }

        require(StringUtils.isNoneEmpty(mongoCollection), "mongo collection require")
        require(StringUtils.isNotEmpty(hiveTable), "hive table require")

        log.info("args:" + args.mkString(" "))
        log.info("mongoCollection:" + mongoCollection)
        log.info("hiveTable:" + hiveTable)
        log.info("columns:" + columns)
        log.info("where:" + where)
        log.info("excludeColumns:" + excludeColumns)
        log.info("partition:" + partition)


        val sparkConf = new SparkConf().setAppName("Mongo2Hive")
        val sparkContext = new SparkContext(sparkConf)

        val sqlContext = new SQLContext(sparkContext)

        val config = ReadConfig(Map(
            "spark.mongodb.input.uri" -> "mongodb://192.168.0.42:20001",
            "spark.mongodb.input.database" -> "beeper2",
            "spark.mongodb.input.collection" -> mongoCollection,
            "spark.mongodb.input.partitioner" -> "MongoSinglePartitioner"
        ))

        val df = MongoSpark.load(sqlContext, config)

        var hiveDF = df

        // 选择字段
        if (StringUtils.isNotEmpty(columns)) {
            val columnArray = columns.split(",")
            val colNames = columnArray.map(name => col(name))
            hiveDF = hiveDF.select(colNames: _*)
        }

        // where 条件
        if (StringUtils.isNotEmpty(where)) {
            val whereExpr = WhereExpr.parse(where).get
            val left = whereExpr.left.value
            val right = whereExpr.right.value
            val op = whereExpr.operator.value
            val column = op match {
                case ">" => col(left) > right
                case "<" => col(left) < right
                case "=" => col(left) === right
                case ">=" => col(left) >= right
                case "<=" => col(left) <= right
                case "!=" => col(left) !== right
            }

            hiveDF = hiveDF.where(column)
        }

        // 过滤字段
        if (StringUtils.isNotEmpty(excludeColumns)) {
            val columnArray = excludeColumns.split(",").map(name => name.trim())
            val lastNames = hiveDF.schema.fieldNames.filter(name => !columnArray.contains(name))
            val colNames = lastNames.map(name => col(name))
            hiveDF = hiveDF.select(colNames: _*)
        }

        var partitionKey = ""
        // 有partition 操作，默认增加一列
        if (StringUtils.isNotEmpty(partition)) {
            sqlContext.setConf("hive.exec.dynamic.partition", "true")
            sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
            val partitionArray = partition.trim().split("=")
            partitionKey = partitionArray(0).trim()
            val partitionValue = partitionArray(1).trim()
            hiveDF = hiveDF.withColumn(partitionKey, lit(partitionValue))
        }

        log.info(hiveDF.explain())

        if (StringUtils.isNotEmpty(partition)) {
            hiveDF.write.mode(SaveMode.Append).partitionBy(partitionKey)
            .saveAsTable(hiveTable)
        } else {
            hiveDF.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(hiveTable)
        }

        sparkContext.stop()

    }

}
