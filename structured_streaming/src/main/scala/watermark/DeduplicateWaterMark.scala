package watermark

import base.StructuredBase
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.OutputMode
import util.DateUtil

/**
1,2018-03-06 10:52:07
2,2018-03-06 10:52:07
1,2018-03-06 10:59:07
1,2018-03-06 10:49:07
  */
object DeduplicateWaterMark extends StructuredBase {
    override protected val appName: String = "DeduplicateWaterMark"

    def main(args: Array[String]): Unit = {

        import spark.implicits._

        addListener()

        val duplicates = socketData
        .filter(line => StringUtils.isNoneEmpty(line))
        .map(line => {
            val array = line.split(",")
            (array(0),DateUtil.dateStr2Timestamp(array(1)))
        })
        .toDF("id","timestamp")
        /**
          * 如果使用watermark , 会根据watermark 规则移除state保存的数据
          */
        .withWatermark("timestamp", "1 minute")
        .dropDuplicates("id","timestamp")

        val query = consoleShow(duplicates, 10, OutputMode.Append())

        query.awaitTermination()

    }
}
