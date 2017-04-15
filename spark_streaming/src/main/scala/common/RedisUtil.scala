package common

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by yxl on 17/4/15.
  */
object RedisUtil extends Serializable {
  val redisHost = "10.10.4.130"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
