package monitor

import java.net._
import java.util.Date
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import base.Log
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}
import org.spark_project.jetty.server._
import org.spark_project.jetty.server.handler.{AbstractHandler, ContextHandler, ContextHandlerCollection}
import org.spark_project.jetty.util.thread.{QueuedThreadPool}
import util.NetWorkUtil

import scala.util.Random


class ShutdownHandler(server:Server,streamingQuery: StreamingQuery) extends AbstractHandler with Log{
    override def handle(target: String, baseRequest: Request,
                        request: HttpServletRequest,
                        response: HttpServletResponse): Unit = {
        response.setContentType("text/json; charset=utf-8")
        response.setStatus(HttpServletResponse.SC_OK)
        val queryName = streamingQuery.name
        logger.info(s"停止StreamingQuery:${queryName}")

        try {
            // 处理逻辑
            streamingQuery.stop()

            val out = response.getWriter
            out.print(s"${queryName} has shutdown")
            out.close()
        } catch {
            case e:Exception => {
                logger.error(s"停止StreamingQuery:${queryName}异常",e)
            }
        } finally {
            MonitorServer.stopServer(server)
        }
    }
}

class StatusHandler(server:Server,streamingQuery: StreamingQuery) extends AbstractHandler with Log {
    override def handle(target: String, baseRequest: Request,
                        request: HttpServletRequest,
                        response: HttpServletResponse): Unit = {
        response.setContentType("text/json; charset=utf-8")
        response.setStatus(HttpServletResponse.SC_OK)
        logger.info(s"查询StreamingQuery:${streamingQuery.name}")
        val queryStatus = streamingQuery.lastProgress
        val out = response.getWriter
        out.print(queryStatus)
        out.close()
    }
}

class TimeHandler extends AbstractHandler with Log {
    override def handle(target: String, baseRequest: Request,
                        request: HttpServletRequest,
                        response: HttpServletResponse): Unit = {
        response.setContentType("text/json; charset=utf-8")
        response.setStatus(HttpServletResponse.SC_OK)
        val date = new Date()
        val out = response.getWriter
        out.print(date)
        out.close()
    }
}

object MonitorServer extends Log {

    private def randomInt(min: Int, max: Int): Int = {
        val item = Random.nextInt(65536)
        if (item <= 1024) {
            item + 1024
        } else {
            item
        }
    }

    private def isBindCollision(exception: Throwable): Boolean = {
        exception match {
            case e: BindException =>
                if (e.getMessage != null) {
                    return true
                }
                isBindCollision(e.getCause)
            case e: Exception => isBindCollision(e.getCause)
            case _ => false
        }
    }

    private def createConnector(server:Server,host:String,port: Int): (ServerConnector, Int) = {
        val connector = new ServerConnector(server)
        connector.setPort(port)
        connector.setHost(host)
        connector.setAcceptQueueSize(math.min(connector.getAcceptors,4))
        connector.start()
        (connector, connector.getLocalPort)
    }

    def startServer(port: Int, streamingQuery: StreamingQuery): (String,Int,Server) = {
        require(port == 0 || (1024 <= port && port < 65536), s"启动端口非法:${port}")
        val host = NetWorkUtil.getLocalAddress()
        val maxRetry = 3

        val pool = new QueuedThreadPool
        pool.setDaemon(true)
        val server = new Server(pool)

        val stopContextHandler = new ContextHandler()
        stopContextHandler.setContextPath("/shutdown")
        stopContextHandler.setHandler(new ShutdownHandler(server,streamingQuery))

        val statusContextHandler = new ContextHandler()
        statusContextHandler.setContextPath("/status")
        statusContextHandler.setHandler(new StatusHandler(server,streamingQuery))

        val timeContextHandler = new ContextHandler()
        timeContextHandler.setContextPath("/time")
        timeContextHandler.setHandler(new TimeHandler)

        val contexts = new ContextHandlerCollection()
        contexts.setHandlers(Array[Handler](timeContextHandler,stopContextHandler,statusContextHandler))
        server.setHandler(contexts)

        server.start()

        def startOnPort(port:Int): (ServerConnector,Int) = {
            for (index <- 0 to maxRetry) {
                val tryPort = if (port == 0) {
                    randomInt(1024, 65536)
                } else {
                    if(index > 0) randomInt(1024, 65536) else port
                }
                try {
                    val (connector, startPort) = createConnector(server,host,tryPort)
                    return (connector, startPort)
                } catch {
                    case e: Exception if isBindCollision(e) => {
                        if (index >= maxRetry) {
                            throw e
                        }
                        logger.info(s"端口:${tryPort}被占用随机生成[1024,65536)端口")
                    }
                }
            }
            throw new Exception("启动jetty服务器异常")
        }

        val (connector, startPort) = startOnPort(port)

        server.addConnector(connector)

        return (host,startPort,server)
    }

    def stopServer(server:Server): Unit ={
        try{
            server.stop()
        }catch{
            case e:Exception =>{
                logger.error("停止jetty 异常",e)
            }
        }
    }

}
