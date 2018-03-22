package monitor

import base.Log
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}

/**
  * Created by yxl on 2018/3/22.
  */
object MonitorImplicits extends Serializable with Log {

    implicit class StreamingQueryWithMonitor(streamingQuery: StreamingQuery) {
        /**
          * 启动 jetty server 提供监控(status) 停止(shutdown) 时间(time) http 访问接口
          * @param userPort
          */
        def monitorAndAwaitTermination(userPort: Int = 0): Unit = {
            assert(streamingQuery != null, "StreamingQuery 为空")
            val (host, port,server) = MonitorServer.startServer(userPort, streamingQuery)
            logger.info(s"启动jetty 服务器 ${host}:${port}")
            try {
                streamingQuery.awaitTermination()
            } catch {
                case e: StreamingQueryException => {
                    logger.error("停止StreamingQuery异常", e)
                }
            }finally{
                MonitorServer.stopServer(server)
            }
        }
    }
}
